package mokey_typist.search

import akka.actor.{Actor, ActorLogging, ActorRef, PoisonPill, Props}
import mokey_typist.util.RandomCharGeneratorFactory

import scala.concurrent.Promise
import scala.concurrent.duration.FiniteDuration
import scala.util.Success

class Master(randomCharGeneratorFactory: RandomCharGeneratorFactory) extends Actor with ActorLogging with IgnoreStateActor {
  private var workReport: Map[WorkerId, WorkerReportContent] = Map.empty
  private var workers: Set[ActorRef] = Set.empty
  private var reportPromise: Promise[MasterResponse] = null

  private def spawnWorkers(lookupString: String, totalWorkers: Int, timeout: FiniteDuration): Unit = {
    for (id <- 0 to totalWorkers) {
      val worker = context.system.actorOf(Props(classOf[Worker], randomCharGeneratorFactory.get, lookupString, timeout, id, self))
      workers += worker
      log.info(s"Spawned worker $id")
      worker ! Start
    }
  }

  override def receive = waiting

  private def waiting: Receive = {
    case StartLookup(lookupString, totalWorkers, timeout, inReportPromise) =>
      reportPromise = inReportPromise

      log.info(s"Starting search process of $lookupString with $totalWorkers workers within ${timeout.toMillis}[ms]")

      spawnWorkers(lookupString, totalWorkers, timeout)
      context.become(inProgress)
  }

  private def inProgress: Receive = {
    case StartLookup(lookupString, totalWorkers, timeout, inReportPromise) =>
      log.info(s"Ignoring request for search of $lookupString with $totalWorkers workers within ${timeout.toMillis}[ms]...")
      inReportPromise.complete(Success(InProgressResponse))

    case WorkerReportMessage(workerId, reportContent) =>
      val worker = sender
      workers -= worker
      workReport += (workerId -> reportContent)
      worker ! PoisonPill

      log.info(s"Worker $workerId reports: $reportContent")

      if(workers.isEmpty) {
        log.info(s"Process is finished, sending final report, ready to process new request")
        reportPromise.complete(Success(MasterReportResponse(workReport)))
        context.become(waiting)
      } else {
        log.info(s"Workers left: ${workers.size}")
      }
  }
}
