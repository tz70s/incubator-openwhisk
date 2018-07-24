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

package whisk.core.scheduler

import akka.actor.{Actor, ActorRef, ActorSystem, PoisonPill, Props}
import akka.cluster.singleton.{ClusterSingletonManager, ClusterSingletonManagerSettings}
import akka.stream.ActorMaterializer
import whisk.common.{Logging, LoggingMarkers, TransactionId}
import whisk.core.{ConfigKeys, WhiskConfig}
import pureconfig.loadConfigOrThrow
import whisk.core.connector.ActivationMessage
import whisk.core.containerpool._
import whisk.core.containerpool.future.ContainerFactoryProtocol._
import whisk.core.entity._
import akka.event.Logging.InfoLevel

import scala.concurrent.Future
import scala.util.{Failure, Success}

object SchedulerSingleton {

  def props(config: WhiskConfig)(implicit system: ActorSystem, logging: Logging) =
    ClusterSingletonManager.props(
      schedulerProps(config),
      terminationMessage = PoisonPill,
      settings = ClusterSingletonManagerSettings(system))

  private def schedulerProps(config: WhiskConfig)(implicit logging: Logging) =
    Props(new SchedulerSingleton(config))
}

class SchedulerSingleton(config: WhiskConfig)(implicit logging: Logging) extends Actor {
  import spray.json._
  implicit val entityNameSerdes = FullyQualifiedEntityName.serdes

  implicit val system = context.system
  implicit val ec = context.system.dispatcher
  implicit val materializer = ActorMaterializer()

  private[this] val entityStore = WhiskEntityStore.datastore()
  private[this] val authStore = WhiskAuthStore.datastore()
  private[this] val namespaceBlacklist = new NamespaceBlacklist(authStore)

  whisk.common.Scheduler
    .scheduleWaitAtMost(loadConfigOrThrow[NamespaceBlacklistConfig](ConfigKeys.blacklist).pollInterval) { () =>
      logging.debug(this, "running background job to update blacklist")
      namespaceBlacklist.refreshBlacklist()(ec, TransactionId.invoker).andThen {
        case Success(set) => logging.info(this, s"updated blacklist to ${set.size} entries")
        case Failure(t)   => logging.error(this, s"error on updating the blacklist: ${t.getMessage}")
      }
    }

  private[this] val pool =
    context.actorOf(ContainerPool.props(config))

  private[this] var overflowProxies = Map[ControllerInstanceId, ActorRef]()
  private[this] var shareStatesProxies = Map[ControllerInstanceId, ActorRef]()

  override def receive: Receive = {
    case OverflowProxyRegistration(instanceId) =>
      overflowProxies = overflowProxies + (instanceId -> sender())
    case ShareStatesProxyRegistration(instanceId) =>
      shareStatesProxies = shareStatesProxies + (instanceId -> sender())

    case ContainerRequisition(activationMessageString, instanceId) =>
      // Process an container creation:
      // The response is pipe back via two channels:
      // 1. ScheduleResult: which sent via ContainerPool after Scheduling.
      // 2. ScheduleFailure: any error occurred in this.
      Future(ActivationMessage.parse(activationMessageString))
        .flatMap(Future.fromTry)
        .flatMap { msg =>
          implicit val transid = msg.transid
          logging.info(this, s"receive overflow msg ${msg.serialize}")
          processActivation(msg, instanceId)
        }
        .recoverWith {
          case e: Throwable =>
            logging.error(this, s"error occurred in dealing with scheduling, ${e.getMessage}")
            sender() ! ScheduleFailure(e)
            Future.failed(e)
        }

    case ScheduleResult(data, ctx) =>
      // when a new container being created, we may desire to broadcast this into all proxies.
      val actionId =
        FullyQualifiedEntityName(data.action.namespace, data.action.name, Some(data.action.version)).toJson.compactPrint
      logging.info(this, s"schedule result: $actionId, belongs to ${ctx.ownedby}")
      overflowProxies.get(ctx.ownedby).foreach { proxy =>
        proxy ! ContainerAllocation(actionId, data.container.ctx)
      }

    case SchedulerRejection(job) =>
      val actionId = job.action.fullyQualifiedName(true).toJson.compactPrint
      logging.info(this, s"scheduler rejection, system overloaded ...")
      overflowProxies.get(job.controllerInstanceId).foreach { proxy =>
        proxy ! ContainerAllocationRejection(actionId)
      }

    case ContainerFreeState(ctx) =>
      pool ! ContainerFreeState(ctx)

    case ContainerBusyState(ctx) =>
      pool ! ContainerBusyState(ctx)

    case NotifyRemoval(data, ctx) =>
      shareStatesProxies.get(ctx.ownedby) match {
        case Some(ref) =>
          val actionId =
            FullyQualifiedEntityName(data.action.namespace, data.action.name, Some(data.action.version)).toJson.compactPrint
          ref ! ContainerDeletion(actionId, ctx)
        case None =>
          throw new Exception("share states proxy belongs to this container context didn't register correctly.")
      }
  }

  def processActivation(msg: ActivationMessage, instanceId: ControllerInstanceId)(
    implicit transid: TransactionId): Future[Unit] = {
    if (!namespaceBlacklist.isBlacklisted(msg.user)) {
      val start = transid.started(this, LoggingMarkers.INVOKER_ACTIVATION, logLevel = InfoLevel)
      val namespace = msg.action.path
      val name = msg.action.name
      val actionid = FullyQualifiedEntityName(namespace, name).toDocId.asDocInfo(msg.revision)
      val subject = msg.user.subject

      WhiskAction
        .get(entityStore, actionid.id, actionid.rev, fromCache = actionid.rev != DocRevision.empty)
        .flatMap { action =>
          action.toExecutableWhiskAction match {
            case Some(executable) =>
              pool ! Run(executable, msg, instanceId)
              Future.successful(())
            case None =>
              Future.failed(new Exception("Error occurred on transforming action into executable."))
          }
        }
    } else {
      // black list
      Future.failed(new Exception(s"The user ${msg.user} is blocked by namespace black list"))
    }
  }
}
