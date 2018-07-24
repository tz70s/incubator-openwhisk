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

import scala.collection.immutable
import whisk.common.{AkkaLogging, LoggingMarkers, TransactionId}
import akka.actor.{Actor, Props}
import pureconfig.loadConfigOrThrow
import whisk.core.containerpool.future.ContainerContext
import whisk.core.{ConfigKeys, WhiskConfig}
import whisk.core.entity._
import whisk.core.entity.size._
import akka.pattern.pipe
import whisk.core.containerpool.future.ContainerFactoryProtocol.{ContainerBusyState, ContainerFreeState}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

/**
 * The core scheduling algorithm takes place here, note that this is not optimal and may not be safe.
 * A prototype that reuse part of prior invoker logic.
 *
 * First, we remove the concept on node affinity,
 * by contrast, the container pool transparently manage container with a logical big pool.
 *
 * Second, no job scheduling, only creation(initialization) and deletion.
 * Therefore, we don't really need most of ContainerProxy logic.
 *
 * The pool management is reused by prior free, warm and busy pool algorithm.
 *
 * Upon actor creation, the pool will start to prewarm containers according
 * to the provided prewarmConfig, iff set. Those containers will **not** be
 * part of the poolsize calculation, which is capped by the poolSize parameter.
 * Prewarm containers are only used, if they have matching arguments
 * (kind, memory) and there is space in the pool.
 */
class ContainerPool(config: WhiskConfig) extends Actor {
  implicit val system = context.system
  implicit val logging = new AkkaLogging(context.system.log)
  implicit val ec = system.dispatcher

  private[this] val poolConfig =
    loadConfigOrThrow[whisk.core.containerpool.future.ContainerPoolConfig](ConfigKeys.containerPool)
  private[this] val maximumContainers = poolConfig.maxActiveContainers
  private[this] val prewarmConfig = ExecManifest.runtimesManifest.stemcells.flatMap {
    case (mf, cells) =>
      cells.map { cell =>
        PrewarmingConfig(cell.count, CodeExecAsString(mf, "", None), cell.memory)
      }
  }.toList

  var freePool = immutable.Map.empty[ContainerContext, ContainerData]
  var busyPool = immutable.Map.empty[ContainerContext, ContainerData]
  var prewarmedPool = immutable.Map.empty[ContainerContext, ContainerData]
  val logMessageInterval = 10.seconds

  // debugging
  whisk.common.Scheduler
    .scheduleWaitAtMost(1.second) { () =>
      logging.info(
        this,
        s"Current size - free pool : ${freePool.size}, busy pool: ${busyPool.size}, prewarm: ${prewarmedPool.size}")
      Future.successful(())
    }
  private[this] val twoLevelScheduler = TwoLevelScheduler(config)

  override def postStop(): Unit = {
    implicit val transid = TransactionId.invoker
    val futures = (freePool ++ prewarmedPool ++ busyPool).map {
      case (_, data: PreWarmedData) =>
        data.container.destroy()
      case (_, data: WarmedData) =>
        data.container.destroy()
      case _ =>
        Future.successful(())
    }
    Await.ready(Future.sequence(futures), 30.seconds)
  }

  prewarmConfig.foreach { config =>
    logging.info(this, s"pre-warming ${config.count} ${config.exec.kind} ${config.memoryLimit.toString}")(
      TransactionId.invokerWarmup)
    (1 to config.count).foreach { _ =>
      createPrewarmContainer(config.exec, config.memoryLimit)
    }
  }

  // TODO: should mofify this logging marker to be correct.
  def logContainerStart(r: Run, containerState: String): Unit = {
    val namespaceName = r.msg.user.namespace.name
    val actionName = r.action.name.name
    val activationId = r.msg.activationId.toString
    val controllerInstanceId = r.controllerInstanceId

    r.msg.transid.mark(
      this,
      LoggingMarkers.SCHEDULER_START_CONTAINER(containerState),
      s"containerStart containerState: $containerState action: $actionName namespace: $namespaceName activationId: $activationId controllerInstanceId: $controllerInstanceId",
      akka.event.Logging.InfoLevel)
  }

  def receive: Receive = {

    // Schedule a job to a container
    //
    // The algorithm is different to prior version, since I make it rely on future now.
    // But we still need to ensure container pools being thread-safe.
    // Simply done this by ensuring all pool operations occurred in non-future scoped, forced sequential via actor channels.
    //
    // First, find existed:
    // 1. find the best match, matching action type and user namespace name from free pool.
    // 2. find the prewarmed container with matching action type.
    //
    // Second, create one:
    // 1. create one container, a.k.a cold start
    // 2. remove a container from free pool and recreate given one.
    case r: Run =>
      val exist = if (busyPool.size < poolConfig.maxActiveContainers) {
        ContainerPool
          .schedule(r.action, r.msg.user.namespace.name, freePool)
          .map { container =>
            busyPool = busyPool + (container._1 -> container._2)
            freePool = freePool - container._1
            self ! CreateWarmedContainer(container._1, container._2)
            (container, "warm")
          }
          .orElse {
            if (busyPool.size + freePool.size < poolConfig.maxActiveContainers) {
              takePrewarmContainer(r.action)
                .map { container =>
                  // take the position first, will override data once it's got initialized.
                  freePool = freePool + (container._1 -> container._2)
                  prewarmedPool = prewarmedPool - container._1
                  (container, "prewarm")
                }
            } else None
          }
      } else None

      exist match {
        case Some(((ctx, data: WarmedData), "warm")) =>
        // do nothing, we already pipe back to parent.
        case Some(((ctx, data: PreWarmedData), "prewarm")) =>
          twoLevelScheduler
            .initialize(data.container, r)
            .map(data => TakePrewarmedContainer(ctx, data))
            .pipeTo(self)
        case Some(data) =>
          throw new Exception(s"error occurred in container pool scheduling ... wrong matching data: $data")
        case None =>
          // First, try to create one.
          if (busyPool.size + freePool.size < poolConfig.maxActiveContainers) {
            twoLevelScheduler
              .createAndInitialize(r)
              .map(data => CreateWarmedContainer(data.container.ctx, data))
              .pipeTo(self)
          } else {
            // in this case, no optimization currently, simply try to remove one and requeue.
            ContainerPool.remove(freePool) match {
              case Some(toDelete) =>
                removeContainer(toDelete).map(_ => RescheduleJob(Some(toDelete), r)).pipeTo(self)
              case None =>
                context.parent ! SchedulerRejection(r)
            }
          }
      }

    case TakeWarmedContainer(ctx, data: WarmedData) =>
      context.parent ! ScheduleResult(data, ctx)

    case TakePrewarmedContainer(ctx, data) =>
      busyPool = busyPool + (ctx -> data)
      freePool = freePool - ctx
      context.parent ! ScheduleResult(data, ctx)

    // Container is already warmed up, pipe back to singleton
    case CreateWarmedContainer(ctx, data: WarmedData) =>
      busyPool = busyPool + (ctx -> data)
      freePool = freePool - ctx
      context.parent ! ScheduleResult(data, ctx)

    // Container is prewarmed and ready to take work
    case PrewarmedAvailable(ctx, data: PreWarmedData) =>
      prewarmedPool = prewarmedPool + (ctx -> data)

    // This message is received for one of these reasons:
    // 1. Container errored while resuming a warm container, could not process the job, and sent the job back
    // 2. The container aged, is destroying itself, and was assigned a job which it had to send back
    // 3. The container aged and is destroying itself
    // Update the free/busy lists but no message is sent to the feed since there is no change in capacity yet
    case RescheduleJob(ctx, job) =>
      ctx match {
        case Some(c) => freePool = freePool - c
        case None    => // no removal occurred.
      }
      val isErrorLogged = job.retryLogDeadline.forall(_.isOverdue())
      val retryLogDeadline = if (isErrorLogged) {
        logging.error(
          this,
          s"Rescheduling Run message, too many message in the pool, freePoolSize: ${freePool.size}, " +
            s"busyPoolSize: ${busyPool.size}, maxActiveContainers ${poolConfig.maxActiveContainers}, " +
            s"userNamespace: ${job.msg.user.namespace.name}, action: ${job.action}")(job.msg.transid)
        Some(logMessageInterval.fromNow)
      } else {
        job.retryLogDeadline
      }
      self ! Run(job.action, job.msg, job.controllerInstanceId, retryLogDeadline)

    case ContainerFreeState(ctx) =>
      busyPool.get(ctx) match {
        case Some(data) =>
          freePool = freePool + (ctx -> data)
          busyPool = busyPool - ctx
        case None =>
      }

    case ContainerBusyState(ctx) =>
      freePool.get(ctx) match {
        case Some(data) =>
          busyPool = busyPool + (ctx -> data)
          freePool = freePool - ctx
        case None =>
      }
  }

  /** Creates a new prewarmed container */
  def createPrewarmContainer(exec: CodeExec[_], memoryLimit: ByteSize) = {
    logging.info(this, s"Spawn an prewarm container ${exec.image}")
    twoLevelScheduler
      .prewarm(Start(exec, memoryLimit))
      .map(prewarm => PrewarmedAvailable(prewarm.container.ctx, prewarm))
      .pipeTo(self)
  }

  /**
   * Takes a prewarm container out of the prewarmed pool
   * iff a container with a matching kind is found.
   */
  def takePrewarmContainer(action: ExecutableWhiskAction): Option[(ContainerContext, ContainerData)] = {
    val kind = action.exec.kind
    val memory = action.limits.memory.megabytes.MB
    prewarmedPool
      .find {
        case (_, PreWarmedData(_, `kind`, `memory`)) => true
        case _                                       => false
      }
      .map {
        case (ctx, data) =>
          // Move the container to the usual pool
          freePool = freePool + (ctx -> data)
          prewarmedPool = prewarmedPool - ctx
          // Since we take out one prewarmed container, create a new prewarm container.
          if (busyPool.size + freePool.size + prewarmedPool.size + 1 < maximumContainers) {
            createPrewarmContainer(action.exec, memory)
          }
          ctx -> data
      }
  }

  /** Removes a container and updates state accordingly. */
  def removeContainer(ctx: ContainerContext) = {
    logging.info(this, "remove a container")
    implicit val transid = TransactionId.invokerNanny
    freePool.get(ctx) match {
      case Some(data: WarmedData) =>
        context.parent ! NotifyRemoval(data, ctx)
        twoLevelScheduler.removeContainer(data.container)
      case Some(data: PreWarmedData) =>
        // notify removal?
        twoLevelScheduler.removeContainer(data.container)
      case _ => Future.successful(()) // actually remove nothing, will this be some kind of dangling?
    }
  }
}

object ContainerPool {

  /**
   * Finds the best container for a given job to run on.
   *
   * Selects an arbitrary warm container from the passed pool of idle containers
   * that matches the action and the invocation namespace. The implementation uses
   * matching such that structural equality of action and the invocation namespace
   * is required.
   * Returns None iff no matching container is in the idle pool.
   * Does not consider pre-warmed containers.
   */
  protected[containerpool] def schedule[A](action: ExecutableWhiskAction,
                                           invocationNamespace: EntityName,
                                           idles: Map[A, ContainerData]): Option[(A, ContainerData)] = {
    idles.find {
      case (_, WarmedData(_, `invocationNamespace`, `action`, _)) => true
      case _                                                      => false
    }
  }

  /**
   * Finds the oldest previously used container to remove to make space for the job passed to run.
   *
   * NOTE: This method is never called to remove an action that is in the pool already,
   * since this would be picked up earlier in the scheduler and the container reused.
   */
  protected[containerpool] def remove[A](pool: Map[A, ContainerData]): Option[A] = {
    val freeContainers = pool.collect {
      case (context, w: WarmedData) => context -> w
    }

    if (freeContainers.nonEmpty) {
      val (context, _) = freeContainers.minBy(_._2.lastUsed)
      Some(context)
    } else None
  }

  def props(config: WhiskConfig) = Props(new ContainerPool(config))
}

/** Contains settings needed to perform container prewarming. */
case class PrewarmingConfig(count: Int, exec: CodeExec[_], memoryLimit: ByteSize)
case class ScheduleResult(data: WarmedData, ctx: ContainerContext)
case class ScheduleFailure(e: Throwable)
case class NotifyRemoval(data: WarmedData, ctx: ContainerContext)
