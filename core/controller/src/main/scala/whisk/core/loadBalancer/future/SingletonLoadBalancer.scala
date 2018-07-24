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

package whisk.core.loadBalancer.future

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import whisk.common.{Logging, TransactionId}
import whisk.core.WhiskConfig
import whisk.core.connector.ActivationMessage
import whisk.core.entity._
import whisk.core.loadBalancer.{InvokerHealth, LoadBalancer, LoadBalancerProvider}

import scala.concurrent.Future

class SingletonLoadBalancer extends LoadBalancer {

  override def publish(action: ExecutableWhiskActionMetaData, msg: ActivationMessage)(
    implicit transid: TransactionId): Future[Future[Either[ActivationId, WhiskActivation]]] = ???

  override def activeActivationsFor(namespace: UUID): Future[Int] = ???

  override def totalActiveActivations: Future[Int] = ???

  override def invokerHealth(): Future[IndexedSeq[InvokerHealth]] = ???
}

object SingletonLoadBalancer extends LoadBalancerProvider {
  override def requiredProperties: Map[String, String] = Map.empty

  override def instance(whiskConfig: WhiskConfig, instance: ControllerInstanceId)(
    implicit actorSystem: ActorSystem,
    logging: Logging,
    materializer: ActorMaterializer): LoadBalancer = ???
}
