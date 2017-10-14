package orchestrator.pipeline

import org.joda.time.DateTime

import scala.concurrent.duration.FiniteDuration

/**
  * Job settings
  * @param parallelism desired parallelism. Will be executed sequentially if not enough workers for parallel execution
  *                    with desired parallelism.
  * @param retries number of retries
  * @param dependsOn list of jobs that should be finished with success before this job can be sheduled for execution
  * @param executionTimeLimit optional execution time limit
  */
case class JobSettings(parallelism: Int,
                       retries: Int,
                       dependsOn: List[JobID],
                       executionTimeLimit: Option[DateTime],
                       executionInterval: Option[FiniteDuration])

/**
  * Job Description
  * @param id unique job identifier
  * @param `class` job class. For simplicity we assume that job class contains all the information required for
  *                job execution and payload deserialization
  * @param payload serialized payload
  * @param settings execution settings can be used to build dependency DAG
  */
case class JobDescription(id: JobID, `class`: JobClass, payload: Payload, settings: JobSettings)

object JobStatus extends Enumeration {
  val PENDING, IN_PROGRESS, WAITING, FINISHED, FAILED, UNKNOWN = Value
}

/**
  * Job report that worker sends to Master for progress tracking and to keep records
  * @param progress progress, value in range from 0 to 1
  * @param status job status
  * @param message message that describes progress of the job
  */
case class JobReport(progress: Double, status: JobStatus, message: String, timestamp: DateTime)

object JobReport {
  def generate: JobReport = JobReport(0D, JobStatus.PENDING, Messages.PENDING_EXECUTION, DateTime.now)
  def apply(jobStatus: JobStatus, message: String): JobReport = JobReport(0D, jobStatus, message, DateTime.now)
}

case class JobInfo(description: JobDescription, reportHistory: Seq[JobReport])

object JobInfo {
  def apply(description: JobDescription): JobInfo = JobInfo(description, List(JobReport.generate))
}