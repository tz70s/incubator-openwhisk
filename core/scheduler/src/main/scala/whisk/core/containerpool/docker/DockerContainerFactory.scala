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

package whisk.core.containerpool.docker

import akka.actor.ActorSystem

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import whisk.common.Logging
import whisk.common.TransactionId
import whisk.core.WhiskConfig
import whisk.core.containerpool.future._
import whisk.core.entity.{ByteSize, ControllerInstanceId, ExecManifest, InvokerInstanceId}

import scala.concurrent.duration._
import java.util.concurrent.TimeoutException

import pureconfig._
import whisk.core.ConfigKeys
import whisk.core.containerpool.{ContainerAddress, ContainerArgsConfig}

import scala.util.{Failure, Success}

class DockerContainerFactory(val instance: InvokerInstanceId,
                             parameters: Map[String, Set[String]],
                             containerArgsConfig: ContainerArgsConfig =
                               loadConfigOrThrow[ContainerArgsConfig](ConfigKeys.containerArgs))(
  implicit actorSystem: ActorSystem,
  ec: ExecutionContext,
  logging: Logging,
  docker: DockerApi)
    extends ContainerFactory {

  private[this] val poolConfig = loadConfigOrThrow[ContainerPoolConfig](ConfigKeys.containerPool).pools(instance.toInt)

  private[this] val remoteAddress = poolConfig.daemon.map(_.host).getOrElse("localhost")

  // In order to know the available slots, temporary introduce a synchronized variable.
  // Note that: we should avoid this by establish a well-defined abstraction.
  // The available slots varies from max number of containers to 0
  @volatile var slots = List.empty[ContainerContext]
  def availableSlots = poolConfig.coreShare * poolConfig.numCore - slots.length

  /** Create a container using docker cli */
  override def createContainer(
    tid: TransactionId,
    name: String,
    actionImage: ExecManifest.ImageName,
    userProvidedImage: Boolean,
    memory: ByteSize,
    cpuShares: Int,
    port: Int,
    controller: ControllerInstanceId)(implicit config: WhiskConfig, logging: Logging): Future[CoarseGrainContainer] = {

    DockerContainer
      .create(
        tid,
        image =
          if (userProvidedImage) Left(actionImage) else Right(actionImage.localImageName(config.runtimesRegistry)),
        memory = memory,
        cpuShares = cpuShares,
        environment = Map("__OW_API_HOST" -> config.wskApiHost),
        network = containerArgsConfig.network,
        dnsServers = containerArgsConfig.dnsServers,
        name = Some(name),
        parameters ++ containerArgsConfig.extraArgs.map { case (k, v) => ("--" + k, v) },
        addr = ContainerAddress(remoteAddress, port),
        invokerId = instance,
        controllerId = controller)
      .andThen {
        case Success(c) => slots = c.ctx :: slots
        case Failure(e) => logging.error(this, s"Failed to create action container: ${e.getMessage}")
      }
  }

  override def removeContainer(container: CoarseGrainContainer)(implicit transid: TransactionId): Future[Unit] = {
    container.destroy().andThen {
      case Success(_) => slots = slots.dropWhile(container.ctx == _)
      case Failure(e) =>
        logging.error(this, s"Failed to remove action container ${container.ctx.containerId}: ${e.getMessage}")
    }
  }

  /** Perform cleanup on init */
  override def init(): Unit = removeAllActionContainers()

  /** Perform cleanup on exit - to be registered as shutdown hook */
  override def cleanup(): Unit = {
    implicit val transid = TransactionId.invoker
    try {
      removeAllActionContainers()
    } catch {
      case e: Exception => logging.error(this, s"Failed to remove action containers: ${e.getMessage}")
    }
  }

  /**
   * Removes all wsk_ containers - regardless of their state
   *
   * If the system in general or Docker in particular has a very
   * high load, commands may take longer than the specified time
   * resulting in an exception.
   *
   * There is no checking whether container removal was successful
   * or not.
   *
   * @throws InterruptedException     if the current thread is interrupted while waiting
   * @throws TimeoutException         if after waiting for the specified time this `Awaitable` is still not ready
   */
  @throws(classOf[TimeoutException])
  @throws(classOf[InterruptedException])
  private def removeAllActionContainers(): Unit = {
    implicit val transid = TransactionId.invoker
    val cleaning = slots.map { ctx =>
      logging.info(this, s"removing ${slots.size} action containers.")
      docker.rm(ctx.containerId).andThen {
        case Success(_) => slots = slots.dropWhile(ctx == _)
        case Failure(e) =>
          logging.error(this, s"Error occurred on removing container ${ctx.containerId}, ${e.getMessage}")
      }
    }
    Await.ready(Future.sequence(cleaning), 30.seconds)
  }
}

object DockerContainerFactoryProvider extends ContainerFactoryProvider {
  override def instance(actorSystem: ActorSystem,
                        logging: Logging,
                        config: WhiskConfig,
                        instanceId: InvokerInstanceId,
                        parameters: Map[String, Set[String]]): ContainerFactory = {

    val daemon = loadConfigOrThrow[ContainerPoolConfig](ConfigKeys.containerPool)
      .pools(instanceId.toInt)
      .daemon
      .map(config => s"${config.host}:${config.port}")

    new DockerContainerFactory(instanceId, parameters)(
      actorSystem,
      actorSystem.dispatcher,
      logging,
      new DockerClientWithFileAccess(daemon)(actorSystem.dispatcher)(logging, actorSystem))
  }
}
