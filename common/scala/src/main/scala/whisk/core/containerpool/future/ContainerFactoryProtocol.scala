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

package whisk.core.containerpool.future

import whisk.core.entity.ControllerInstanceId

object ContainerFactoryProtocol {

  /** Request a container with activation message for a more sophisticated decision. */
  case class ContainerRequisition(activationMessage: String, controllerInstanceId: ControllerInstanceId)

  case class ContainerAllocation(actionSerde: String, ctx: ContainerContext)

  case class ContainerAllocationRejection(actionSerde: String)

  case class ContainerBusyState(ctx: ContainerContext)

  case class ContainerFreeState(ctx: ContainerContext)

  case class ContainerDeletion(actionSerde: String, ctx: ContainerContext)

  case class OverflowProxyRegistration(instanceId: ControllerInstanceId)

  case class ShareStatesProxyRegistration(instanceId: ControllerInstanceId)
}
