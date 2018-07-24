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

import whisk.common.TransactionId
import whisk.core.database.StaleParameter
import whisk.core.entity.{Identity, View}
import whisk.core.entity.types.AuthStore

import scala.concurrent.{ExecutionContext, Future}
import spray.json.DefaultJsonProtocol._

import scala.concurrent.duration.FiniteDuration

/** No logic is changed in namespace blacklist, currently. */
class NamespaceBlacklist(authStore: AuthStore) {

  private var blacklist: Set[String] = Set.empty

  def isBlacklisted(identity: Identity): Boolean = blacklist.contains(identity.namespace.name.asString)

  def refreshBlacklist()(implicit ec: ExecutionContext, tid: TransactionId): Future[Set[String]] = {
    authStore
      .query(
        table = NamespaceBlacklist.view.name,
        startKey = List.empty,
        endKey = List.empty,
        skip = 0,
        limit = Int.MaxValue,
        includeDocs = false,
        descending = true,
        reduce = false,
        stale = StaleParameter.UpdateAfter)
      .map(_.map(_.fields("key").convertTo[String]).toSet)
      .map { newBlacklist =>
        blacklist = newBlacklist
        newBlacklist
      }
  }
}

object NamespaceBlacklist {
  val view = View("namespaceThrottlings", "blockedNamespaces")
}

case class NamespaceBlacklistConfig(pollInterval: FiniteDuration)
