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

import java.time.Instant

import akka.actor.ActorSystem

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import whisk.common.Logging
import whisk.common.TransactionId
import whisk.core.entity.ActivationResponse.{ConnectionError, MemoryExhausted}
import whisk.core.entity.{ActivationEntityLimit, ByteSize, ControllerInstanceId, InvokerInstanceId}
import whisk.core.entity.size._
import spray.json._
import whisk.core.containerpool.future.{CoarseGrainContainer, ContainerContext}
import whisk.core.containerpool._
import whisk.core.entity.ExecManifest.ImageName
import whisk.http.Messages

object DockerContainer {

  /**
   * Creates a container running on a docker daemon.
   *
   * @param transid transaction creating the container
   * @param image either a user provided (Left) or OpenWhisk provided (Right) image
   * @param memory memorylimit of the container
   * @param cpuShares sharefactor for the container
   * @param environment environment variables to set on the container
   * @param network network to launch the container in
   * @param dnsServers list of dns servers to use in the container
   * @param name optional name for the container
   * @return a Future which either completes with a DockerContainer or one of two specific failures
   */
  def create(transid: TransactionId,
             image: Either[ImageName, String],
             memory: ByteSize = 256.MB,
             cpuShares: Int = 0,
             environment: Map[String, String] = Map.empty,
             network: String = "bridge",
             dnsServers: Seq[String] = Seq.empty,
             name: Option[String] = None,
             dockerRunParameters: Map[String, Set[String]],
             addr: ContainerAddress,
             invokerId: InvokerInstanceId,
             controllerId: ControllerInstanceId)(implicit docker: DockerApi,
                                                 as: ActorSystem,
                                                 ec: ExecutionContext,
                                                 log: Logging): Future[DockerContainer] = {
    implicit val tid: TransactionId = transid

    val environmentArgs = environment.flatMap {
      case (key, value) => Seq("-e", s"$key=$value")
    }

    val params = dockerRunParameters.flatMap {
      case (key, valueList) => valueList.toList.flatMap(Seq(key, _))
    }

    val args = Seq(
      "--cpu-shares",
      cpuShares.toString,
      "--memory",
      s"${memory.toMB}m",
      "--memory-swap",
      s"${memory.toMB}m",
      "--publish",
      s"${addr.port}:8080",
      "--network",
      network) ++
      environmentArgs ++
      dnsServers.flatMap(d => Seq("--dns", d)) ++
      name.map(n => Seq("--name", n)).getOrElse(Seq.empty) ++
      params

    val imageToUse = image.fold(_.publicImageName, identity)

    val pulled = image match {
      case Left(userProvided) if userProvided.tag.map(_ == "latest").getOrElse(true) =>
        // Iff the image tag is "latest" explicitly (or implicitly because no tag is given at all), failing to pull will
        // fail the whole container bringup process, because it is expected to pick up the very latest "untagged"
        // version every time.
        docker.pull(imageToUse).map(_ => true).recoverWith {
          case _ => Future.failed(BlackboxStartupError(Messages.imagePullError(imageToUse)))
        }
      case Left(_) =>
        // Iff the image tag is something else than latest, we tolerate an outdated image if one is available locally.
        // A `docker run` will be tried nonetheless to try to start a container (which will succeed if the image is
        // already available locally)
        docker.pull(imageToUse).map(_ => true).recover { case _ => false }
      case Right(_) =>
        // Iff we're not pulling at all (OpenWhisk provided image) we act as if the pull was successful.
        Future.successful(true)
    }

    for {
      pullSuccessful <- pulled
      id <- docker.run(imageToUse, args).recoverWith {
        case BrokenDockerContainer(brokenId, _) =>
          // Remove the broken container - but don't wait or check for the result.
          // If the removal fails, there is nothing we could do to recover from the recovery.
          docker.rm(brokenId)
          Future.failed(WhiskContainerStartupError(Messages.resourceProvisionError))
        case _ =>
          // Iff the pull was successful, we assume that the error is not due to an image pull error, otherwise
          // the docker run was a backup measure to try and start the container anyway. If it fails again, we assume
          // the image could still not be pulled and wasn't available locally.
          if (pullSuccessful) {
            Future.failed(WhiskContainerStartupError(Messages.resourceProvisionError))
          } else {
            Future.failed(BlackboxStartupError(Messages.imagePullError(imageToUse)))
          }
      }
    } yield new DockerContainer(ContainerContext(id, addr, invokerId, controllerId))
  }
}

/**
 * Represents a container as run by docker.
 *
 * This class contains OpenWhisk specific behavior and as such does not necessarily
 * use docker commands to achieve the effects needed.
 */
class DockerContainer(override val ctx: ContainerContext)(implicit docker: DockerApi,
                                                          override protected val as: ActorSystem,
                                                          protected val ec: ExecutionContext,
                                                          protected val logging: Logging)
    extends CoarseGrainContainer {

  private[this] val waitForOomState: FiniteDuration = 2.seconds
  private[this] val filePollInterval: FiniteDuration = 5.milliseconds

  override def destroy()(implicit transid: TransactionId): Future[Unit] = {
    docker.rm(ctx.containerId).flatMap(_ => super.destroy())
  }

  /**
   * Was the container killed due to memory exhaustion?
   *
   * Retries because as all docker state-relevant operations, they won't
   * be reflected by the respective commands immediately but will take
   * some time to be propagated.
   *
   * @param retries number of retries to make
   * @return a Future indicating a memory exhaustion situation
   */
  private def isOomKilled(retries: Int = (waitForOomState / filePollInterval).toInt)(
    implicit transid: TransactionId): Future[Boolean] = {
    docker.isOomKilled(ctx.containerId)(TransactionId.invoker).flatMap { killed =>
      if (killed) Future.successful(true)
      else if (retries > 0) akka.pattern.after(filePollInterval, as.scheduler)(isOomKilled(retries - 1))
      else Future.successful(false)
    }
  }

  override protected def callContainer(path: String, body: JsObject, timeout: FiniteDuration, retry: Boolean = false)(
    implicit transid: TransactionId): Future[RunResult] = {
    val started = Instant.now()
    val http = httpConnection.getOrElse {
      val conn = if (config.akkaClient) {
        new AkkaContainerClient(
          ctx.containerAddress.host,
          ctx.containerAddress.port,
          timeout,
          ActivationEntityLimit.MAX_ACTIVATION_ENTITY_LIMIT,
          1024)
      } else {
        new ApacheBlockingContainerClient(
          s"${ctx.containerAddress.host}:${ctx.containerAddress.port}",
          timeout,
          ActivationEntityLimit.MAX_ACTIVATION_ENTITY_LIMIT)
      }
      httpConnection = Some(conn)
      conn
    }

    http
      .post(path, body, retry)
      .flatMap { response =>
        val finished = Instant.now()

        response.left
          .map {
            // Only check for memory exhaustion if there was a
            // terminal connection error.
            case error: ConnectionError =>
              isOomKilled().map {
                case true  => MemoryExhausted()
                case false => error
              }
            case other => Future.successful(other)
          }
          .fold(_.map(Left(_)), right => Future.successful(Right(right)))
          .map(res => RunResult(Interval(started, finished), res))
      }
  }
}
