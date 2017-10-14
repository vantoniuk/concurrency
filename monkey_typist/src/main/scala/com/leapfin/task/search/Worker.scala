package com.leapfin.task.search

import akka.actor.{Actor, ActorLogging, ActorRef, PoisonPill}
import com.leapfin.task.util.RandomCharGenerator
import org.joda.time.DateTime

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util.{Failure, Success}

/**
  * index of the char in lookup string that we need to check for match, assuming all elements before such index matched
  * @param index index
  */
case class MatchChar(index: Int)
case object Start
case object Timeout

class Worker(randomCharGenerator: RandomCharGenerator,
             lookupString: String,
             timeout: FiniteDuration,
             id: Int,
             master: ActorRef) extends Actor with ActorLogging with IgnoreStateActor {
  private implicit val ctx = context.system.dispatcher

  private var bytesRead: Long = 0
  private var startTime: DateTime = null

  override def receive: Receive = init

  private def init: Receive = {
    case Start =>
      context.become(process orElse ignore)
      context.system.scheduler.schedule(timeout, timeout, self, Timeout)
      startTime = DateTime.now()

      log.info(s"Worker $id started at $startTime")

      self ! MatchChar(0)
  }

  private def matchChar(index: Int, char: Char): Unit = {
    if(char == lookupString.charAt(index)) {
      if(index == lookupString.length - 1) {
        val elapsedTimeInMillis = DateTime.now.getMillis - startTime.getMillis
        val duration = FiniteDuration(elapsedTimeInMillis, "ms")
        log.info(s"Match found, worder ID: $id, elapsed time: $elapsedTimeInMillis[ms]")
        stopAndReportToMaster(SuccessWorkerReport(bytesRead, duration))
      } else {
        if(index > 1) log.debug(s"Worker $id found ${lookupString.substring(0, index + 1)} match")
        self ! MatchChar(index + 1)
      }
    } else { // we start from index 0
      self ! MatchChar(0)
    }
  }

  private def process: Receive = {
    case MatchChar(index) =>
      randomCharGenerator.getNext match {
        case Success(char) =>
          bytesRead += 2
          matchChar(index, char)
        case Failure(error) =>
          log.error(s"Error occurred, worker ID: $id", error)
          stopAndReportToMaster(FailureWorkerReport(error))
      }
    case Timeout =>
      stopAndReportToMaster(TimeoutWorkerReport)
  }

  private def stopAndReportToMaster(reportContent: WorkerReportContent): Unit = {
    context.become(ignore)
    master ! WorkerReportMessage(WorkerId(id), reportContent)
  }

}
