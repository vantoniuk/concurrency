package orchestrator.pipeline

import akka.actor.ActorRef

import scala.concurrent.Promise

object Messages {

  val CLASS_NOT_SUPPORTED = "job class in not supported"

  val NOT_ENOUGH_RESOURCES = "worker doesn't have resources for requested job class execution"

  val EXECUTION_STARTED = "worker started execution"

  val PENDING_EXECUTION = "job is pending execution"

  val NO_WORKERS = "no workers available for that job class"

  val NO_FREE_SLOTS = "no more jobs can be accepted for interval based execution"

  val HEALTH_CHECK_ID: JobID = "healthcheck"

  /********* External Messages ******************/
  case class ScheduleJob(jobDescription: JobDescription, result: Promise[ScheduleJobResult])

  object ScheduleJob {
    def apply(jobDescription: JobDescription): ScheduleJob = ScheduleJob(jobDescription, Promise())
  }

  /********* Master - INTERNAL Messages ************/
  case object ReschedulePendingJobs

  case object RescheduleIntervalJobs

  case object WorkerHealthCheck

  case class ActivateWorker(worker: ActorRef, workerID: WorkerID)

  /********* Master - Worker Messages ************/

  trait ScheduleJobResult

  // Acknowledge the job execution
  case class Ack(jobID: JobID) extends ScheduleJobResult

  // Refuse job due to specified reason, like worker at capacity etc.
  case class Refuse(jobID: JobID, reason: String) extends ScheduleJobResult

  // Suspend job due to not satisfied dependent jobs.
  case class Suspend(jobID: JobID) extends ScheduleJobResult

  case class RequestSent(jobID: JobID) extends ScheduleJobResult

  case class GetJobReport(jobID: JobID)

  case class JobReportUpdate(workerID: WorkerID, jobID: JobID, jobReport: JobReport)

  case class RegisterExecutables(workerID: WorkerID, jobClasses: List[(JobClass, JobWeight)])

  case class StartJob(jobDescription: JobDescription)

  case object HealthCheck

  /********* Master to Keeper Messages ************/

  case class WorkerError(workerID: WorkerID, message: String)

  case class UpdateState(jobDescription: JobDescription, report: JobReport)

}
