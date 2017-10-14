package orchestrator.pipeline

import orchestrator.util.Loggable

import scala.concurrent.Future

/**
  * General interface for job state keeper.
  * State keeper provides access to job records, which is required to satisfy dependency constraints and
  * to ensure durability of orchestrator
  */
trait JobStateKeeper {
  def jobsByStatus(statuses: Set[JobStatus]): Future[List[JobInfo]]

  def getById(jobID: JobID): Future[Option[JobInfo]]

  def save(job: JobInfo): Future[Boolean]

  def update(jobID: JobID, jobReport: JobReport): Future[Boolean]
  
}

object InMemoryJobStateKeeper extends JobStateKeeper with Loggable {
  private var jobInfoMap: Map[JobID, JobInfo] = Map.empty

  def getById(jobID: JobID): Future[Option[JobInfo]] = {
    val result = jobInfoMap.get(jobID)

    if(result.isDefined) {
      log.info(s"Job $jobID found")
    } else {
      log.info(s"Job $jobID not found")
    }

    Future.successful(result)
  }

  def save(job: JobInfo): Future[Boolean] = {
    log.info(s"Job ${job.description.id} saved")
    jobInfoMap += ((job.description.id, job))
    Future.successful(true)
  }

  def update(jobID: JobID, jobReport: JobReport): Future[Boolean] = {
    jobInfoMap.get(jobID) match {
      case None =>
        log.error(s"Job $jobID not found, update failed")
        Future.successful(false)
      case Some(jobInfo) =>
        log.info(s"Job $jobID updated")
        jobInfoMap += ((jobID, jobInfo.copy(reportHistory = jobReport +: jobInfo.reportHistory)))
        Future.successful(true)
    }
  }

  def jobsByStatus(statuses: Set[JobStatus]): Future[List[JobInfo]] = {
    Future.successful(
      jobInfoMap
        .values // for all values of the map
        .toList
        // take jobs with status that is contained in provided set
        .filter(_.reportHistory.headOption.exists(report => statuses.contains(report.status)))
    )
  }
}
