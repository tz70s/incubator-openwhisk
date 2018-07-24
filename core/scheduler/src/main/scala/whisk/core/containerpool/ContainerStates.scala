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

package whisk.core.containerpool

import java.time.Instant

import scala.concurrent.duration._
import whisk.core.connector.ActivationMessage
import whisk.core.containerpool.future.{CoarseGrainContainer, ContainerContext}
import whisk.core.entity._

// Data
sealed abstract class ContainerData(val lastUsed: Instant)
case class NoData() extends ContainerData(Instant.EPOCH)
case class PreWarmedData(container: CoarseGrainContainer, kind: String, memoryLimit: ByteSize)
    extends ContainerData(Instant.EPOCH)
case class WarmedData(container: CoarseGrainContainer,
                      invocationNamespace: EntityName,
                      action: ExecutableWhiskAction,
                      override val lastUsed: Instant)
    extends ContainerData(lastUsed)

// Events received by the actor
case class Start(exec: CodeExec[_], memoryLimit: ByteSize)
case class Run(action: ExecutableWhiskAction,
               msg: ActivationMessage,
               controllerInstanceId: ControllerInstanceId,
               retryLogDeadline: Option[Deadline] = None)
case object Remove

// Channels for safely doing mutable operations.
case class TakeWarmedContainer(ctx: ContainerContext, data: WarmedData)
case class TakePrewarmedContainer(ctx: ContainerContext, data: WarmedData)

// Events sent by the actor
case class CreateWarmedContainer(ctx: ContainerContext, data: ContainerData)
case class PrewarmedAvailable(ctx: ContainerContext, data: PreWarmedData)
case class ContainerRemoved(ctx: ContainerContext)
case class RescheduleJob(toDelete: Option[ContainerContext], job: Run) // job is sent back to parent and could not be processed because container is being destroyed
case class SchedulerRejection(job: Run)
