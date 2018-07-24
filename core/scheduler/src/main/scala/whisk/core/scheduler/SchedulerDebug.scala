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

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.server.Directives._
import whisk.core.connector.ActivationMessage

class WhiskSchedulerDebugDirectives extends SprayJsonSupport {

  import ActivationMessage.serdes

  private[this] val info =
    pathEndOrSingleSlash {
      complete("Hi, this is an debug directive.")
    }

  private[this] val creation =
    (path("create") & post) {
      entity(as[ActivationMessage]) { active =>
        complete(s"$active")
      }
    }

  val route = creation ~ info
}
