package com.leapfin.task

import akka.actor.{ActorSystem, Props}
import com.leapfin.task.search._
import com.leapfin.task.util.{Loggable, RandomCharGeneratorFactory}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Future, Promise}

trait StringLookup {
  def lookup(lookupString: String, totalWorkers: Int, timeout: FiniteDuration): Future[String]
  def terminate(): Unit
}

class ActorStringLookup(randomCharGeneratorFactory: RandomCharGeneratorFactory, system: ActorSystem) extends StringLookup with Loggable {
  private val masterActor = system.actorOf(Props(classOf[Master], randomCharGeneratorFactory))
  private implicit val ctx = system.dispatcher

  def terminate(): Unit = system.terminate()

  def lookup(lookupString: String, totalWorkers: Int, timeout: FiniteDuration) = {
    val reportPromise = Promise[MasterResponse]

    masterActor ! StartLookup(lookupString, totalWorkers, timeout, reportPromise)

    reportPromise.future.flatMap(printReport)
  }

  private def printReport(response: MasterResponse): Future[String] = {
    response match {
      case InProgressResponse =>
        log.error(s"Master actor is in unexpected state, returning an error!")
        Future.failed(new IllegalStateException("Can't perform lookup, unexpected error occurred"))

      case MasterReportResponse(workReport) =>
        val reportList = workReport.values.toList
        val successReports = reportList
          .collect({case report: SuccessWorkerReport => report })
          .sortBy(- _.elapsedTime.toMillis) // sorting by elapsed time in descending order

        val timeoutReports = reportList
          .collect({case report @ TimeoutWorkerReport => report })

        val failedReports = reportList
          .collect({case report: FailureWorkerReport => report })

        val averageBytesPerMs = if(successReports.isEmpty) {
          0D
        } else {
          successReports.map(r => r.bytesRead.toDouble / r.elapsedTime.toMillis).sum / successReports.length
        }

        val reportString = (successReports ++ timeoutReports ++ failedReports).mkString("\n", "\n", "\n")


        Future.successful(s"$reportString$averageBytesPerMs")
    }
  }
}
