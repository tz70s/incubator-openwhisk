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
import java.time.Instant

import akka.stream.scaladsl.Source
import akka.util.ByteString
import pureconfig._

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.util.Failure
import scala.util.Success
import spray.json.JsObject
import spray.json.DefaultJsonProtocol._
import whisk.common.Logging
import whisk.common.LoggingMarkers
import whisk.common.TransactionId
import whisk.core.entity._
import whisk.http.Messages
import akka.event.Logging.InfoLevel
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpRequest, Uri}
import akka.http.scaladsl.model.Uri.Path
import whisk.core.ConfigKeys
import whisk.core.containerpool._

case class ContainerContext(containerId: ContainerId,
                            containerAddress: ContainerAddress,
                            sitsin: InvokerInstanceId,
                            var ownedby: ControllerInstanceId)
case class ContainerPoolClient(akkaClient: Boolean)

/**
 * In the future architecture, the Container abstraction will be slightly different.
 *
 * The prior abstraction will be divided into three pieces for different usage and visible from different components.
 * First, the controller side will only know the run, pause and resume command, since it only knows warmed containers.
 *
 * Second, the log, pause and run will be placed into InvokerAgent logic.
 * In this case, log will be no longer visible in either Controller nor Scheduler.
 *
 * Third, the Scheduler takes remain logic, responsible for creating/deleting containers and initialization.
 *
 * Therefore, split this into three pieces.
 */
trait ContainerRestCall {
  implicit protected val as: ActorSystem
  val ctx: ContainerContext
  protected implicit val logging: Logging
  protected implicit val ec: ExecutionContext

  /** Config value for kind of http client */
  protected[containerpool] val config = loadConfigOrThrow[ContainerPoolClient](ConfigKeys.containerPoolClient)

  /** HTTP connection to the container, will be lazily established by callContainer */
  protected var httpConnection: Option[ContainerClient] = None

  // I think this should be refactored, by removing timeout and retry, but need to ensure where the variation occurred.
  // If removed, we can make httpConnection without var.
  protected def callContainer(path: String, body: JsObject, timeout: FiniteDuration, retry: Boolean = false)(
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
      .map { response =>
        val finished = Instant.now()
        RunResult(Interval(started, finished), response)
      }
  }
}

trait ContainerRunnable extends ContainerRestCall {

  protected val invokerAgentAddress: String
  protected val invokerAgentPort: Int

  /** Stops the container from consuming CPU cycles. */
  def suspend()(implicit transid: TransactionId): Future[Unit]

  /** Dual of halt. */
  def resume()(implicit transid: TransactionId): Future[Unit]

  protected def callInvokerAgent(path: String, timeout: FiniteDuration, retry: Boolean = false)(
    implicit transid: TransactionId) = {
    val uri = Uri()
      .withScheme("http")
      .withHost(invokerAgentAddress)
      .withPort(invokerAgentPort)
      .withPath(Path / path / ctx.containerId.asString)

    Http().singleRequest(HttpRequest(uri = uri)) flatMap { response =>
      if (response.status.isSuccess()) Future.successful(())
      else Future.failed(new Exception(s"call invoker agent failed, status: ${response.status.intValue()}"))
    }
  }

  /** Runs code in the container. */
  def run(parameters: JsObject, environment: JsObject, timeout: FiniteDuration)(
    implicit transid: TransactionId): Future[(Interval, ActivationResponse)] = {
    val actionName = environment.fields.get("action_name").map(_.convertTo[String]).getOrElse("")
    val start =
      transid.started(
        this,
        LoggingMarkers.INVOKER_ACTIVATION_RUN,
        s"sending arguments to $actionName at ${ctx.containerId} ${ctx.containerAddress}",
        logLevel = InfoLevel)

    val parameterWrapper = JsObject("value" -> parameters)
    val body = JsObject(parameterWrapper.fields ++ environment.fields)
    callContainer("/run", body, timeout, retry = false)
      .andThen { // never fails
        case Success(r: RunResult) =>
          transid.finished(
            this,
            start.copy(start = r.interval.start),
            s"running result: ${r.toBriefString}",
            endTime = r.interval.end,
            logLevel = InfoLevel)
        case Failure(t) =>
          transid.failed(this, start, s"run failed with $t")
      }
      .map { result =>
        val response = if (result.interval.duration >= timeout) {
          ActivationResponse.applicationError(Messages.timedoutActivation(timeout, false))
        } else {
          ActivationResponse.processRunResponseContent(result.response, logging)
        }

        (result.interval, response)
      }
  }
}

trait ContainerLog {

  /** Obtains logs up to a given threshold from the container. Optionally waits for a sentinel to appear. */
  def logs(limit: ByteSize, waitForSentinel: Boolean)(implicit transid: TransactionId): Source[ByteString, Any]
}

trait CoarseGrainContainer extends ContainerRestCall {

  /** Initializes code in the container. */
  def initialize(initializer: JsObject, timeout: FiniteDuration)(implicit transid: TransactionId): Future[Interval] = {
    val start = transid.started(
      this,
      LoggingMarkers.INVOKER_ACTIVATION_INIT,
      s"sending initialization to ${ctx.containerId} ${ctx.containerAddress}",
      logLevel = InfoLevel)

    val body = JsObject("value" -> initializer)
    callContainer("/init", body, timeout, retry = true)
      .andThen { // never fails
        case Success(r: RunResult) =>
          transid.finished(
            this,
            start.copy(start = r.interval.start),
            s"initialization result: ${r.toBriefString}",
            endTime = r.interval.end,
            logLevel = InfoLevel)
        case Failure(t) =>
          transid.failed(this, start, s"initialization failed with $t")
      }
      .flatMap { result =>
        if (result.ok) {
          Future.successful(result.interval)
        } else if (result.interval.duration >= timeout) {
          Future.failed(
            InitializationError(
              result.interval,
              ActivationResponse.applicationError(Messages.timedoutActivation(timeout, true))))
        } else {
          Future.failed(
            InitializationError(
              result.interval,
              ActivationResponse.processInitResponseContent(result.response, logging)))
        }
      }
  }

  /** Completely destroys this instance of the container. */
  def destroy()(implicit transid: TransactionId): Future[Unit] = {
    Future.successful(httpConnection.foreach(_.close()))
  }
}
