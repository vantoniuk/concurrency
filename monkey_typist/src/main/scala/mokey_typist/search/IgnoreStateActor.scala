package mokey_typist.search

import akka.actor.{Actor, ActorLogging}

trait IgnoreStateActor extends Actor with ActorLogging{
  protected def ignore: Receive = {
    case message => log.info(s"Ignoring [$message] ")
  }
}
