package orchestrator.pipeline.actor

import akka.actor.ActorRef
import orchestrator.pipeline.Messages._
import orchestrator.pipeline.{JobID, JobReport, JobStatus, Worker}

import scala.util.{Failure, Success, Try}

/**
  * Actor wrapper. Used to register actual worker in system for orchestration
  * @param worker instance of [[Worker]]
  * @param master master actor
  */
class WorkerActor(worker: Worker, master: ActorRef) extends IgnoreStateActor {
  private implicit val ctx = context.dispatcher

  // request executable job classes and job weight from actual worker
  worker.executableJobClasses.onComplete(futureHandler(
    s"worker ${worker.id} registering executable jobs",
    s"worker ${worker.id} can't be registered"
  ){executables => master ! RegisterExecutables(worker.id, executables)})

  // here we process messages that we can, or ignore and log otherwise
  def receive = process orElse ignore

  def process: Receive = {
    case StartJob(jobDescription) =>
      worker.execute(jobDescription, onCallbackReceived).onComplete(futureHandler(
        s"worker ${worker.id} received start job request ${jobDescription.id} ${jobDescription.`class`}",
        s"worker ${worker.id} can't execute job ${jobDescription.id} ${jobDescription.`class`}"
      ){registerJobResponse => master ! registerJobResponse})

    case GetJobReport(jobID) =>
      worker.getReport(jobID).onComplete(futureHandler(
        s"worker ${worker.id} sends report for $jobID",
        s"worker ${worker.id} can't get report for $jobID"
      ){
        case Some(workerReport) => master ! JobReportUpdate(worker.id, jobID, workerReport)
        case None =>
          val message = s"worker ${worker.id} don't have records for $jobID"
          log.error(message)
          master ! JobReportUpdate(worker.id, jobID, JobReport(JobStatus.UNKNOWN, message))
      })

  }

  private def onCallbackReceived(jobID: JobID, report: JobReport): Unit = {
    log.info(s"worker ${worker.id} reports $report")
    master ! JobReportUpdate(worker.id, jobID, report)
  }

  private def futureHandler[A](successMessage: String, errorMessage: String)
                              (onSuccess: A => Unit): PartialFunction[Try[A], Unit] = {
    case Success(success) =>
      log.info(successMessage)
      onSuccess(success)
    case Failure(error) =>
      log.error(errorMessage, error)
      master ! WorkerError(worker.id, error.getMessage)
  }
}
