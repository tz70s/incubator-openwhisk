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

package whisk.core.invoker

import akka.Done
import akka.actor.{ActorSystem, CoordinatedShutdown}
import akka.stream.ActorMaterializer
import com.typesafe.config.ConfigValueFactory
import kamon.Kamon
import whisk.common._
import whisk.core.WhiskConfig
import whisk.core.WhiskConfig._
import whisk.core.connector.{MessagingProvider, PingMessage}
import whisk.core.entity.{ExecManifest, InvokerInstanceId}
import whisk.http.{BasicHttpService, BasicRasService}
import whisk.spi.SpiLoader
import whisk.utils.ExecutionContextFactory

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Failure

object Invoker {

  /**
   * An object which records the environment variables required for this component to run.
   */
  def requiredProperties =
    Map(servicePort -> 8080.toString, invokerName -> "", runtimesRegistry -> "") ++
      ExecManifest.requiredProperties ++
      kafkaHosts ++
      zookeeperHosts ++
      wskApiHost

  def initKamon(instance: Int): Unit = {
    // Replace the hostname of the invoker to the assigned id of the invoker.
    val newKamonConfig = Kamon.config
      .withValue(
        "kamon.statsd.simple-metric-key-generator.hostname-override",
        ConfigValueFactory.fromAnyRef(s"invoker$instance"))
    Kamon.start(newKamonConfig)
  }

  def main(args: Array[String]): Unit = {
    implicit val ec = ExecutionContextFactory.makeCachedThreadPoolExecutionContext()
    implicit val actorSystem: ActorSystem =
      ActorSystem(name = "invoker-actor-system", defaultExecutionContext = Some(ec))
    implicit val logger = new AkkaLogging(akka.event.Logging.getLogger(actorSystem, this))

    // Prepare Kamon shutdown
    CoordinatedShutdown(actorSystem).addTask(CoordinatedShutdown.PhaseActorSystemTerminate, "shutdownKamon") { () =>
      logger.info(this, s"Shutting down Kamon with coordinated shutdown")
      Kamon.shutdown()
      Future.successful(Done)
    }

    // load values for the required properties from the environment
    implicit val config = new WhiskConfig(requiredProperties)

    def abort(message: String) = {
      logger.error(this, message)(TransactionId.invoker)
      actorSystem.terminate()
      Await.result(actorSystem.whenTerminated, 30.seconds)
      sys.exit(1)
    }

    if (!config.isValid) {
      abort("Bad configuration, cannot start.")
    }

    val execManifest = ExecManifest.initialize(config)
    if (execManifest.isFailure) {
      logger.error(this, s"Invalid runtimes manifest: ${execManifest.failed.get}")
      abort("Bad configuration, cannot start.")
    }

    val assignedInvokerId = 0
    val invokerName = Some("cluster-singleton-invoker")

    initKamon(assignedInvokerId)

    // TODO: while I've dropped the invokerId, should revise kafka topic too.
    val topicBaseName = "invoker"
    val topicName = topicBaseName + assignedInvokerId
    val invokerInstance = InvokerInstanceId(assignedInvokerId, invokerName, invokerName)
    val msgProvider = SpiLoader.get[MessagingProvider]
    if (msgProvider.ensureTopic(config, topic = topicName, topicConfig = topicBaseName).isFailure) {
      abort(s"failure during msgProvider.ensureTopic for topic $topicName")
    }
    val producer = msgProvider.getProducer(config)
    val invoker = try {
      new InvokerReactive(config, invokerInstance, producer)
    } catch {
      case e: Exception => abort(s"Failed to initialize reactive invoker: ${e.getMessage}")
    }

    Scheduler.scheduleWaitAtMost(1.seconds)(() => {
      producer.send("health", PingMessage(invokerInstance)).andThen {
        case Failure(t) => logger.error(this, s"failed to ping the controller: $t")
      }
    })

    val port = config.servicePort.toInt
    BasicHttpService.startHttpService(new BasicRasService {}.route, port)(
      actorSystem,
      ActorMaterializer.create(actorSystem))
  }
}
