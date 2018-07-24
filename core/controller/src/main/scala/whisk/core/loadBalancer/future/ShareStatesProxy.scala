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

import akka.actor.{Actor, PoisonPill, Props}
import akka.cluster.singleton.{ClusterSingletonProxy, ClusterSingletonProxySettings}
import whisk.core.containerpool.future.ContainerFactoryProtocol.{
  ContainerBusyState,
  ContainerDeletion,
  ContainerFreeState,
  ShareStatesProxyRegistration
}
import whisk.core.entity.{ControllerInstanceId, FullyQualifiedEntityName}

import scala.collection.concurrent.TrieMap

object ShareStatesProxy {
  def props(instanceId: ControllerInstanceId, containers: TrieMap[FullyQualifiedEntityName, Set[ActivationStates]]) =
    Props(new ShareStatesProxy(instanceId, containers))
}

class ShareStatesProxy(instanceId: ControllerInstanceId,
                       private[this] val containers: TrieMap[FullyQualifiedEntityName, Set[ActivationStates]])
    extends Actor {

  import spray.json._
  implicit val entityNameSerdes = FullyQualifiedEntityName.serdes

  private[this] val schedulerActorPath = "/user/scheduler"
  private[this] val schedulerProxy = context.actorOf(
    ClusterSingletonProxy
      .props(singletonManagerPath = schedulerActorPath, settings = ClusterSingletonProxySettings(context.system)),
    name = s"share-states-proxy-${instanceId.asString}")

  schedulerProxy ! ShareStatesProxyRegistration(instanceId)

  override def receive: Receive = {
    case ContainerFreeState(ctx) =>
      schedulerProxy ! ContainerFreeState(ctx)
    case ContainerBusyState(ctx) =>
      schedulerProxy ! ContainerBusyState(ctx)
    case ContainerDeletion(actionSerde, ctx) =>
      val action = actionSerde.parseJson.convertTo[FullyQualifiedEntityName]
      containers.get(action) match {
        case Some(activationStates) =>
          containers(action) = activationStates.filterNot { state =>
            state.proxy ! PoisonPill
            state.ctx == ctx
          }
        case None =>
        // TODO: notify back about failure?
      }
  }
}
