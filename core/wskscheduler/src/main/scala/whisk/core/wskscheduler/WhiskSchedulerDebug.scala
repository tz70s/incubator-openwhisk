package whisk.core.wskscheduler

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
