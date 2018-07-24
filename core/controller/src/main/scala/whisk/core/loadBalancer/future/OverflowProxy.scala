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

import akka.actor.{Actor, ActorRef, Props}
import akka.cluster.singleton.{ClusterSingletonProxy, ClusterSingletonProxySettings}
import whisk.common.{Logging, LoggingMarkers, StartMarker, TransactionId}
import whisk.core.WhiskConfig
import whisk.core.connector.ActivationMessage
import whisk.core.entity.{ControllerInstanceId, FullyQualifiedEntityName}
import whisk.core.loadBalancer.future.OverflowProtocol.{
  ContainerAllocationFailure,
  ContainerAllocationSuccess,
  OverflowActivationMessage
}
import akka.event.Logging.InfoLevel
import whisk.core.containerpool.future.ContainerContext
import whisk.core.containerpool.future.ContainerFactoryProtocol.{
  ContainerAllocation,
  ContainerAllocationRejection,
  ContainerRequisition,
  OverflowProxyRegistration
}

import scala.collection.mutable

object OverflowProtocol {
  case class OverflowActivationMessage(activationMessage: ActivationMessage)(implicit transid: TransactionId)
  case class ContainerAllocationSuccess(action: FullyQualifiedEntityName, ctx: ContainerContext)
  case class ContainerAllocationFailure(cause: Throwable)
}

object OverflowProxy {
  def props(config: WhiskConfig, instanceId: ControllerInstanceId)(implicit logging: Logging) =
    Props(new OverflowProxy(config, instanceId))
}

/**
 * The OverflowProxy is the proxy that communicate with Scheduler in each Controller.
 *
 * Therefore, the responsibility on OverflowProxy:
 * 1. Handle overflow message with buffering.
 * 2. Handling Controller lifecycle and states with OverflowProxy.
 */
class OverflowProxy(config: WhiskConfig, instanceId: ControllerInstanceId)(implicit logging: Logging) extends Actor {

  import spray.json._

  private[this] implicit val entityNameSerdes = FullyQualifiedEntityName.serdes
  private[this] implicit val system = context.system
  private[this] implicit val ec = context.dispatcher

  private[this] val schedulerActorPath = "/user/scheduler"
  private[this] val schedulerProxy = context.actorOf(
    ClusterSingletonProxy
      .props(singletonManagerPath = schedulerActorPath, settings = ClusterSingletonProxySettings(context.system)),
    name = s"overflow-proxy-${instanceId.asString}")

  schedulerProxy ! OverflowProxyRegistration(instanceId)

  // Mutable map and queue are thread safe here.
  private[this] var overflowBuffer = Map[String, mutable.Queue[(ActivationMessage, ActorRef, StartMarker)]]()

  override def receive: Receive = {
    case OverflowActivationMessage(msg) =>
      val startOverflow = msg.transid.started(
        this,
        LoggingMarkers.CONTROLLER_LOADBALANCER_OVERFLOW_QUEUE,
        "can't get container, enqueue in overflow bus",
        InfoLevel)

      // I think there's a better way to handling this ...?
      val entry = overflowBuffer.get(msg.action.toJson.compactPrint)
      val regen = entry match {
        case Some(e) => e
        case None =>
          overflowBuffer = overflowBuffer + (msg.action.toJson.compactPrint -> mutable.Queue.empty)
          overflowBuffer(msg.action.toJson.compactPrint)
      }

      regen.enqueue((msg, sender(), startOverflow))

      // Due to I'm not really sure the required states for scheduling, serde the message currently.
      schedulerProxy ! ContainerRequisition(msg.serialize, instanceId)

    case ContainerAllocation(actionString, ctx) =>
      overflowBuffer.get(actionString).foreach { queue =>
        val (msg, ref, startMarker) = queue.dequeue()
        msg.transid.finished(this, startMarker, "dequeue activation from overflow bus", InfoLevel)
        val action = actionString.parseJson.convertTo[FullyQualifiedEntityName]
        ref ! ContainerAllocationSuccess(action, ctx)
      }

    case ContainerAllocationRejection(actionString) =>
      overflowBuffer.get(actionString).foreach { queue =>
        val (msg, ref, startMarker) = queue.dequeue()
        msg.transid.finished(this, startMarker, "dequeue activation from overflow bus", InfoLevel)
        val action = actionString.parseJson.convertTo[FullyQualifiedEntityName]
        ref ! ContainerAllocationFailure(
          new Exception(s"system overloaded, can't assign workload to action $action, please try again later."))
      }
  }
}
