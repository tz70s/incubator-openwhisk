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

package whisk.core.scheduler

import akka.actor.Actor
import whisk.common.RingBuffer

case class WhiskAgentId(id: String)

// States an WhiskAgent can be in
sealed trait WhiskAgentState {
  val asString: String
  val isUsable: Boolean
}

object WhiskAgentState {
  // WhiskAgent in this state can be used to schedule workload to
  sealed trait Usable extends WhiskAgentState { val isUsable = true }
  // No workload should be scheduled to invokers in this state
  sealed trait Unusable extends WhiskAgentState { val isUsable = false }

  // A completely healthy invoker, pings arriving fine, no system errors
  case object Healthy extends Usable { val asString = "up" }
  // Pings are arriving fine, the WhiskAgent returns system errors though
  case object Unhealthy extends Unusable { val asString = "unhealthy" }
  // Pings are arriving fine, the WhiskAgent does not respond with active-acks in the expected time though
  case object Unresponsible extends Unusable { val asString = "unresponsible" }
  // Pings are not arriving for this WhiskAgent
  case object Offline extends Unusable { val asString = "down" }
}

// Possible answers of an activation
sealed trait InvocationFinishedResult
object InvocationFinishedResult {
  // The activation could be successfully executed from the system's point of view. That includes user- and application
  // errors
  case object Success extends InvocationFinishedResult
  // The activation could not be executed because of a system error
  case object SystemError extends InvocationFinishedResult
  // The active-ack did not arrive before it timed out
  case object Timeout extends InvocationFinishedResult
}

case class InvocationFinishedMessage(result: InvocationFinishedResult)

// Sent to a monitor if the state changed
case class CurrentWhiskAgentPoolState(newState: IndexedSeq[WhiskAgentState])

// Data stored in the Invoker
final case class WhiskAgentInfo(buffer: RingBuffer[InvocationFinishedResult])

/** A proxy actor to reflect the status on the node whisk agent sits in. */
class WhiskAgentProxy(val id: WhiskAgentId) extends Actor {

  override def receive: Receive = ???
}
