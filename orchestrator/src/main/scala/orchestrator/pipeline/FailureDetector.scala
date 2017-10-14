package orchestrator.pipeline

import akka.actor.ActorRef

import scala.concurrent.duration.FiniteDuration

/**
  * Specifies behavior for master when worker is marked as unhealthy
  */
sealed trait RepairStrategy

case object BlackList extends RepairStrategy

case class AvoidInteraction(duration: FiniteDuration) extends RepairStrategy

/**
  * Failure detector. Keeps track of worker response times, job progress etc.
  * The failure detector can implement strategy that tracks worker response time and
  * makes decision based on historical executions. Another strategy could be dependent
  * on worker report frequency, other metrics.
  *
  * For example failure detector can mark worker as unhealthy if some number of jobs
  * failed or some number of executions were refused within some time interval
  *
  */
trait FailureDetector {

  /**
    * List of workers that are suspected to be unhealthy
    * @return list [[ActorRef]]
    */
  def suspectedWorkers: List[(ActorRef, WorkerID)]

  /**
    * List of unhealthy workers
    * @return
    */
  def failedWorkers: List[((ActorRef, WorkerID), RepairStrategy)]

  /**
    * Track the job execution.
    * @param worker worker reference
    * @param jobID job ID
    * @param jobReport report
    */
  def track(worker: ActorRef, workerID: WorkerID, jobID: JobID, jobReport: JobReport): Unit

}

/**
  * Default failure detector. Makes decision based on job status. Can be used for testing only. Not thread safe
  */
object DefaultFailureDetector extends FailureDetector {

  private var suspectedWorkerSet: Set[(ActorRef, WorkerID)] = Set.empty

  private var failedWorkerSet: Set[(ActorRef, WorkerID)] = Set.empty

  def suspectedWorkers: List[(ActorRef, WorkerID)] = suspectedWorkerSet.toList

  def failedWorkers: List[((ActorRef, WorkerID), RepairStrategy)] = failedWorkerSet.toList.map(worker => (worker, BlackList))

  def track(worker: ActorRef, workerID: WorkerID, jobID: JobID, jobReport: JobReport): Unit = {
    jobReport.status match {
      case JobStatus.FAILED => failedWorkerSet += ((worker, workerID))
      case JobStatus.WAITING => suspectedWorkerSet += ((worker, workerID))
      case _ => //ignore
    }
  }
}