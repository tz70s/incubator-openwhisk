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

import akka.actor.ActorSystem
import whisk.core.{ConfigKeys, WhiskConfig}
import pureconfig.loadConfigOrThrow
import whisk.common.{Counter, Logging, TransactionId}
import whisk.core.containerpool.docker.DockerContainerFactory
import whisk.core.containerpool.future.CoarseGrainContainer
import whisk.core.entity.{ControllerInstanceId, InvokerInstanceId}
import whisk.spi.SpiLoader
import whisk.core.entity.size._

import scala.collection.concurrent.TrieMap
import scala.concurrent.Future

/** The TwoLevelScheduler is an abstraction above Docker-based share states; ideally, this should be pushed into Docker SPI impl*/
class TwoLevelScheduler(config: WhiskConfig)(implicit system: ActorSystem, logging: Logging) {

  implicit val whiskconf = config
  implicit val ec = system.dispatcher

  private[this] val poolConfig =
    loadConfigOrThrow[whisk.core.containerpool.future.ContainerPoolConfig](ConfigKeys.containerPool)
  private[this] val poolSize = poolConfig.maxActiveContainers
  private[this] val portBase = 17000
  private[this] val bindPorts = {
    val bindports = TrieMap[Int, Boolean]()
    for (idx <- 0 until poolSize) bindports += (portBase + idx -> false)
    logging.info(this, s"available container ports : $bindports")
    bindports
  }

  private[this] val containerFactories = poolConfig.pools.indices.map { index =>
    val containerFactory = SpiLoader
      .get[whisk.core.containerpool.future.ContainerFactoryProvider]
      .instance(
        system,
        logging,
        config,
        InvokerInstanceId(index),
        Map(
          "--cap-drop" -> Set("NET_RAW", "NET_ADMIN"),
          "--ulimit" -> Set("nofile=1024:1024"),
          "--pids-limit" -> Set("1024")))
    containerFactory.init()
    sys.addShutdownHook(containerFactory.cleanup())
    containerFactory
  }.toList

  /** For large-scale clusters, this is definitely a bad idea. */
  private[this] def availableFactory = {
    containerFactories.maxBy {
      case docker: DockerContainerFactory =>
        docker.availableSlots
      case _ => throw new Exception("kubernetes should not call the available factory abstraction")
    }
  }

  def prewarm(job: Start): Future[PreWarmedData] = {
    bindPorts.find { case (index, used) => !used } match {
      case Some((port, used)) =>
        bindPorts(port) = true
        availableFactory
          .createContainer(
            TransactionId.invokerWarmup,
            TwoLevelScheduler.containerName("prewarm", job.exec.kind),
            job.exec.image,
            job.exec.pull,
            job.memoryLimit,
            poolConfig.cpuShare,
            port,
            ControllerInstanceId("unallocated"))
          .map(container => PreWarmedData(container, job.exec.kind, job.memoryLimit))
      case None =>
        Future.failed(new Exception("no available ports, please ensure container's lifecycle management."))
    }
  }

  def initialize(container: CoarseGrainContainer, job: Run): Future[WarmedData] = {
    implicit val transid = job.msg.transid
    val name = job.action.name
    container
      .initialize(job.action.containerInitializer, job.action.limits.timeout.duration)
      .map(interval => (container, interval))
      .flatMap {
        case (c: CoarseGrainContainer, _) =>
          // TODO: currently, ignore interval
          c.ctx.ownedby = job.controllerInstanceId
          Future(WarmedData(c, job.msg.user.namespace.name, job.action, Instant.now()))
      } recoverWith {
      case e: Throwable =>
        logging.info(this, s"retry for initialization ... ${e.getMessage} occurred")
        initialize(container, job)
    }
  }

  def createAndInitialize(job: Run): Future[WarmedData] = {
    bindPorts.find { case (index, used) => !used } match {
      case Some((port, used)) =>
        bindPorts(port) = true
        implicit val transid = job.msg.transid
        val name = job.action.name
        val container = availableFactory.createContainer(
          job.msg.transid,
          TwoLevelScheduler.containerName(job.msg.user.namespace.name.asString, job.action.name.asString),
          job.action.exec.image,
          job.action.exec.pull,
          job.action.limits.memory.megabytes.MB,
          poolConfig.cpuShare,
          port,
          job.controllerInstanceId)

        container.flatMap { c =>
          initialize(c, job)
        }
      case None =>
        Future.failed(new Exception("no available ports, please ensure container's lifecycle management."))
    }
  }

  def removeContainer(container: CoarseGrainContainer)(implicit transid: TransactionId): Future[Unit] = {
    bindPorts(container.ctx.containerAddress.port) = false
    containerFactories
      .find { case factory: DockerContainerFactory => factory.instance == container.ctx.sitsin }
      .map(_.removeContainer(container))
      .getOrElse(
        Future.failed(new Exception("didn't find the predicate container factory, is this configuration correct?")))
  }
}

object TwoLevelScheduler {
  def apply(config: WhiskConfig)(implicit system: ActorSystem, logging: Logging): TwoLevelScheduler =
    new TwoLevelScheduler(config)

  private val containerCount = new Counter

  def containerName(prefix: String, suffix: String): String = {
    def isAllowed(c: Char): Boolean = c.isLetterOrDigit || c == '_'

    val sanitizedPrefix = prefix.filter(isAllowed)
    val sanitizedSuffix = suffix.filter(isAllowed)

    s"${containerCount.next()}_${sanitizedPrefix}_$sanitizedSuffix"
  }
}
