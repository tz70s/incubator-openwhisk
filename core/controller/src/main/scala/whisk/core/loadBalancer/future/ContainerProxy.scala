/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package whisk.core.loadBalancer.future

import java.time.Instant
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import akka.actor.{Actor, ActorRef, Props, Timers}
import whisk.common.{Logging, TransactionId}
import whisk.core.connector.ActivationMessage
import whisk.core.containerpool.future.{ContainerContext, ContainerRunnable}
import whisk.core.entity.{ExecutableWhiskActionMetaData, WhiskActivation}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import pureconfig.loadConfigOrThrow
import whisk.core.containerpool.future.ContainerFactoryProtocol.{ContainerBusyState, ContainerFreeState}
import akka.pattern.pipe
import scala.concurrent.blocking

sealed trait ContainerState
case object ContainerPaused extends ContainerState
case object ContainerInflight extends ContainerState
case object RaceForPausing extends ContainerState

sealed trait ContainerProxyData
case object Empty extends ContainerProxyData
case class CarrySender(ref: ActorRef) extends ContainerProxyData

object ContainerProxy {
  // TODO: Move these to proper place.
  case class ContainerProxyTimeoutConfig(idleContainer: FiniteDuration, pauseGrace: FiniteDuration)
  case class ContainerProxyConfig(invokerAgentPort: Int, timeouts: ContainerProxyTimeoutConfig)
  val proxyConfig = loadConfigOrThrow[ContainerProxyConfig]("whisk.container-proxy")
  case class CallRun(meta: ExecutableWhiskActionMetaData, msg: ActivationMessage)
  case class RunResult(whiskActivation: WhiskActivation)
  case object PauseKey
  case class PauseTick(transid: TransactionId)
  def props(ctx: ContainerContext, shareStatesProxy: ActorRef)(implicit log: Logging) =
    Props(new ContainerProxy(ctx, shareStatesProxy))
}

class ContainerProxy(override val ctx: ContainerContext, shareStatesProxy: ActorRef)(
  override implicit val logging: Logging)
    extends Actor
    with Timers
    with ContainerRunnable {

  import spray.json._
  import spray.json.DefaultJsonProtocol._
  import ContainerProxy._

  override protected val invokerAgentAddress = ctx.containerAddress.host
  override protected val invokerAgentPort = proxyConfig.invokerAgentPort

  override implicit protected val as = context.system
  override implicit val ec = context.dispatcher

  private[this] val pauseGrace = proxyConfig.timeouts.pauseGrace

  // The critical problem is that how do we manage states, i.e. pause/unpause to Scheduler that it can decide which container is "not busy".
  // i.e.
  // when pausing, container move from busy pool to free pool
  // when resuming, container move from free pool to busy pool
  // will this make Scheduler close to performance bottleneck?

  // TODO: remove hardcoded, use timeout configuration.
  val timeout = 10.seconds
  val internalTimeout = 1.second

  private[this] val inflight = new AtomicInteger(0)
  private[this] val paused = new AtomicBoolean(true)

  override def suspend()(implicit transid: TransactionId) =
    blocking {
      callInvokerAgent("suspend", internalTimeout).recoverWith {
        case e: Throwable =>
          logging.error(this, s"suspend error: ${e.getMessage}")
          // temporarily work around with retry
          callInvokerAgent("suspend", internalTimeout)
      }
    }

  override def resume()(implicit transid: TransactionId): Future[Unit] =
    blocking {
      callInvokerAgent("resume", timeout).recoverWith {
        case e: Throwable =>
          logging.error(this, s"resume error: ${e.getMessage}")
          callInvokerAgent("resume", internalTimeout)
      }
    }

  override def receive: Receive = {
    case CallRun(meta, msg) =>
      timers.cancel(PauseKey)
      implicit val transid = msg.transid
      inflight.incrementAndGet()
      if (paused.get()) {
        Await.ready(resume(), timeout)
        paused.set(false)
        shareStatesProxy ! ContainerBusyState(ctx)
      }
      handleRun(meta, msg)
        .andThen {
          case _ =>
            val value = inflight.decrementAndGet()
            if (value == 0) {
              timers.startSingleTimer(PauseKey, PauseTick, pauseGrace)
            }
        }
        .map(RunResult)
        .pipeTo(sender())

    case PauseTick =>
      implicit val transid = TransactionId.invoker // TODO: what to use?
      if (inflight.get() == 0) {
        shareStatesProxy ! ContainerFreeState(ctx)
        Await.ready(suspend(), timeout)
        paused.set(true)
        timers.cancel(PauseKey)
      } else {
        // reschedule timer due to inflight activation.
        timers.cancel(PauseKey)
        timers.startSingleTimer(PauseKey, PauseTick, pauseGrace)
      }
  }

  private[this] def handleRun(meta: ExecutableWhiskActionMetaData, msg: ActivationMessage)(
    implicit transid: TransactionId) = {
    val timeout = meta.limits.timeout.duration
    val parameters = msg.content getOrElse JsObject.empty
    val authEnvironment = msg.user.authkey.toEnvironment
    val environment = JsObject(
      "namespace" -> msg.user.namespace.name.toJson,
      "action_name" -> msg.action.qualifiedNameWithLeadingSlash.toJson,
      "activation_id" -> msg.activationId.toString.toJson,
      // compute deadline on invoker side avoids discrepancies inside container
      // but potentially under-estimates actual deadline
      "deadline" -> (Instant.now.toEpochMilli + timeout.toMillis).toString.toJson)

    run(parameters, JsObject(authEnvironment.fields ++ environment.fields), timeout)
      .map {
        case (interval, resp) =>
          WhiskActivation(
            activationId = msg.activationId,
            namespace = msg.user.namespace.name.toPath,
            subject = msg.user.subject,
            cause = msg.cause,
            name = msg.action.name,
            version = msg.action.version.get,
            start = interval.start,
            end = interval.end,
            duration = Some(interval.duration.toMillis),
            response = resp,
            annotations = meta.annotations)
      }
  }
}
