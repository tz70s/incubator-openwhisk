package whisk.core.containerpool

import whisk.core.connector.ActivationMessage

object ContainerFactoryProtocol {
  case class ContainerEndpoint(id: ContainerId, address: ContainerAddress)

  /** Request a container with activation message for a more sophisticated decision. */
  case class RequestContainer(activeMsg: ActivationMessage)

  /** Return a list of warmed containers to a specific controller */
  case class ResponseContainerList(list: List[Container])
}
