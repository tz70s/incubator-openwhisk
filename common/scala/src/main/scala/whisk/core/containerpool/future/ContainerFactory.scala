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

import akka.actor.ActorSystem

import scala.concurrent.Future
import whisk.common.Logging
import whisk.common.TransactionId
import whisk.core.WhiskConfig
import whisk.core.entity.{ByteSize, ControllerInstanceId, ExecManifest, InvokerInstanceId}
import whisk.spi.Spi

case class DockerDaemon(host: String, port: Int)
case class SingleContainerPoolConfig(numCore: Int, coreShare: Int, daemon: Option[DockerDaemon])
case class ContainerPoolConfig(pools: List[SingleContainerPoolConfig]) {

  /**
   * The total number of containers is simply the number of cores dilated by the cpu sharing.
   */
  def maxActiveContainers = pools.map(pool => pool.numCore * pool.coreShare).sum

  /** Currently, treat all nodes as homogeneous */
  private val totalShare = 1024.0
  def cpuShare = (totalShare * pools.size / maxActiveContainers).toInt
}

trait ContainerFactory {

  /** create a new Container */
  def createContainer(
    tid: TransactionId,
    name: String,
    actionImage: ExecManifest.ImageName,
    userProvidedImage: Boolean,
    memory: ByteSize,
    cpuShares: Int,
    port: Int,
    controller: ControllerInstanceId)(implicit config: WhiskConfig, logging: Logging): Future[CoarseGrainContainer]

  def removeContainer(container: CoarseGrainContainer)(implicit transid: TransactionId): Future[Unit]

  /** perform any initialization */
  def init(): Unit

  /** cleanup any remaining Containers; should block until complete; should ONLY be run at startup/shutdown */
  def cleanup(): Unit
}

object ContainerFactory {

  /** based on https://github.com/moby/moby/issues/3138 and https://github.com/moby/moby/blob/master/daemon/names/names.go */
  private def isAllowed(c: Char) = c.isLetterOrDigit || c == '_' || c == '.' || c == '-'

  /** include the instance name, if specified and strip invalid chars before attempting to use them in the container name */
  def containerNamePrefix(instanceId: InvokerInstanceId): String =
    s"wsk${instanceId.uniqueName.getOrElse("")}${instanceId.toInt}".filter(isAllowed)
}

trait ContainerFactoryProvider extends Spi {

  // TODO: temporary leave InvokerInstanceId remain, but I think we should remove this as well.
  def instance(actorSystem: ActorSystem,
               logging: Logging,
               config: WhiskConfig,
               instanceId: InvokerInstanceId,
               parameters: Map[String, Set[String]]): ContainerFactory
}
