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

import java.util.concurrent.atomic.LongAdder

import akka.actor.{ActorRef, ActorSystem}
import akka.stream.ActorMaterializer
import whisk.common.{Logging, TransactionId}
import whisk.core.WhiskConfig
import whisk.core.WhiskConfig.kafkaHosts
import whisk.core.connector.ActivationMessage
import whisk.core.entity._
import whisk.core.loadBalancer.future.OverflowProtocol.{
  ContainerAllocationFailure,
  ContainerAllocationSuccess,
  OverflowActivationMessage
}
import whisk.core.loadBalancer.{ActivationEntry, InvokerHealth, LoadBalancer, LoadBalancerProvider}

import scala.collection.concurrent.TrieMap
import scala.concurrent.Future
import akka.pattern.ask
import akka.util.Timeout
import whisk.core.containerpool.future.ContainerContext
import whisk.core.loadBalancer.future.ContainerProxy.{CallRun, RunResult}
import scala.concurrent.blocking

case class ActivationStates(concurrency: Int, ctx: ContainerContext, proxy: ActorRef)

class SingletonLoadBalancer(config: WhiskConfig, instance: ControllerInstanceId)(implicit val actorSystem: ActorSystem,
                                                                                 logging: Logging,
                                                                                 materializer: ActorMaterializer)
    extends LoadBalancer {

  private[this] implicit val ec = actorSystem.dispatcher

  val containers = TrieMap[FullyQualifiedEntityName, Set[ActivationStates]]()

  /** State related to invocations and throttling */
  private[this] val activations = TrieMap[ActivationId, ActivationEntry]()
  private[this] val activationsPerNamespace = TrieMap[UUID, LongAdder]()
  private[this] val totalActivations = new LongAdder()
  private[this] val totalActivationMemory = new LongAdder()

  private[this] val shareStates = actorSystem.actorOf(ShareStatesProxy.props(instance, containers))
  private[this] val overflow = actorSystem.actorOf(OverflowProxy.props(config, instance))

  override def publish(action: ExecutableWhiskActionMetaData, msg: ActivationMessage)(
    implicit transid: TransactionId): Future[Future[Either[ActivationId, WhiskActivation]]] = {

    implicit val timeout = Timeout(action.limits.timeout.duration)

    containers
      .get(msg.action)
      .flatMap(activationStates => activationStates.find(state => state.concurrency < 1)) match {
      case Some(state) =>
        // do something that we can change whole state.
        val statesSet = containers(msg.action) // safe here.
        containers(msg.action) = statesSet - state + ActivationStates(
          concurrency = state.concurrency + 1,
          ctx = state.ctx,
          proxy = state.proxy)
        val result = blocking { state.proxy ? CallRun(action, msg) }
        result.flatMap {
          case RunResult(active) =>
            // once it success, free concurrency
            containers(msg.action) = statesSet + state - ActivationStates(
              concurrency = state.concurrency + 1,
              ctx = state.ctx,
              proxy = state.proxy)
            Future.successful(Future(Right(active)))
        } recoverWith {
          case e: Throwable =>
            logging.error(this, s"Internal error occurred when calling run, ${e.getMessage}")
            Future.failed(e)
        }
      case None =>
        val resource = blocking { overflow ? OverflowActivationMessage(msg) }
        resource.flatMap {
          case ContainerAllocationSuccess(entityName, ctx) =>
            logging.info(
              this,
              s"receive scheduling result of $entityName, send to container $ctx, from controller ${ctx.ownedby}")
            // Create a new container runner actor.
            val proxy = actorSystem.actorOf(ContainerProxy.props(ctx, shareStates))
            // Update list, any better way to avoid mutable variable here?
            containers.get(entityName) match {
              case Some(set) => containers(entityName) = set + ActivationStates(0, ctx, proxy)
              case None      => containers(entityName) = Set.empty + ActivationStates(0, ctx, proxy)
            }
            // Run a recursive call, don't duplicate the look up logic.
            publish(action, msg)
          case ContainerAllocationFailure(cause) => Future.failed(cause)
        }
    }
  }

  override def activeActivationsFor(namespace: UUID): Future[Int] =
    Future.successful(activationsPerNamespace.get(namespace).map(_.intValue()).getOrElse(0))

  override def totalActiveActivations: Future[Int] = Future.successful(totalActivations.intValue())

  /**
   * I'm not sure there is really necessary of providing these routes for?
   * Anyway, since we have a singleton scheduler, contains of global view of cluster states.
   * Therefore, we can make this move to Scheduler for operator to observation:
   * maybe some routes (i.e. current warmed, current prewarm and "Invoker" health, and so on.
   * Similar to AkkaManagement.
   */
  override def invokerHealth(): Future[IndexedSeq[InvokerHealth]] = Future(IndexedSeq.empty)
}

object SingletonLoadBalancer extends LoadBalancerProvider {
  override def requiredProperties: Map[String, String] = kafkaHosts

  override def instance(whiskConfig: WhiskConfig, instance: ControllerInstanceId)(
    implicit actorSystem: ActorSystem,
    logging: Logging,
    materializer: ActorMaterializer): LoadBalancer = new SingletonLoadBalancer(whiskConfig, instance)
}
