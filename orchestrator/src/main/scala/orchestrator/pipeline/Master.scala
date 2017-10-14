package orchestrator.pipeline

import akka.actor.ActorRef
import orchestrator.pipeline.Messages._
import orchestrator.util.Loggable

import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration._
import scala.util.{Failure, Success}

class Master(failureDetector: FailureDetector,
             stateKeeper: JobStateKeeper,
             loadBalancer: LoadBalancer,
             val pendingJobLimit: Int = 1000,
             val intervalJobLimit: Int = 100)(implicit ctx: ExecutionContext) extends Loggable {
  private var workerToJobMap: Map[ActorRef, Set[JobID]] = Map.empty
  private var blackListedWorkerSet: Set[ActorRef] = Set.empty
  private var suspendedWorkerSet: Set[ActorRef] = Set.empty

  // shortcut to count active jobs
  private def activeJobsCount: Int = workerToJobMap.values.map(_.size).sum

  log.info(s"Sheduling historical jobs")
  // reschedule abandoned jobs
  stateKeeper
    .jobsByStatus(Set(JobStatus.PENDING, JobStatus.IN_PROGRESS, JobStatus.WAITING, JobStatus.FAILED, JobStatus.FINISHED))
    .map(jobList => jobList
      .filter(job =>
        // we want to reschedule jobs with some number of retries left
        job.reportHistory.count(_.status == JobStatus.FAILED) <= job.description.settings.retries ||
        // we want to reschedule finished jobs that should be scheduled based on time interval
        job.reportHistory.headOption.exists(_.status == JobStatus.FINISHED) &&
          job.description.settings.executionInterval.isDefined
      )
      .foreach(jobInfo => Await.result(scheduleJob(jobInfo.description), 1 minute)))

  /**
    * Mark worker such that no jobs will be scheduled for execution on it. It can't be
    * activated
    * @param worker worker reference
    * @param workerID worker ID
    * @return option of [[JobDescription]] for future schedule
    */
  def blackListWorker(worker: ActorRef, workerID: WorkerID): Future[Option[JobDescription]] = {
    log.info(s"Moving worker $workerID to blacklist")
    blackListedWorkerSet += worker
    deactivateWorker(worker, workerID)
  }

  /**
    * Mark worker such that no jobs will be scheduled for execution on it until it activated
    * @param worker worker reference
    * @param workerID worker ID
    * @return option of [[JobDescription]] for future schedule
    */
  def suspendWorker(worker: ActorRef, workerID: WorkerID): Future[Option[JobDescription]] = {
    log.info(s"Moving worker $workerID to suspended list")
    suspendedWorkerSet += worker
    deactivateWorker(worker, workerID)
  }

  /**
    * Mark worker as active. So jobs will be scheduled for execution on it.
    * @param worker worker reference
    * @param workerID worker ID
    */
  def activateWorker(worker: ActorRef, workerID: WorkerID): Unit = {
    if(!blackListedWorkerSet.contains(worker)) {
      log.info(s"Activating worker $workerID")
      loadBalancer.activateWorker(worker)
      suspendedWorkerSet -= worker
    } else {
      log.error(s"Worker $workerID was blacklisted, can't be activated")
    }
  }

  /**
    * Register new worker
    * @param worker worker reference
    * @param workerID worker ID
    * @param executableJobs list of jobs with declared weights
    */
  def registerWorker(worker: ActorRef, workerID: WorkerID, executableJobs: List[(JobClass, JobWeight)]): Unit = {
    if(!blackListedWorkerSet.contains(worker) && !suspendedWorkerSet.contains(worker)) {
      log.info(s"Registering worker for ${executableJobs.length} job classes")
      loadBalancer.registerWorker(worker, executableJobs)
    } else {
      log.error(s"Worker $workerID was blacklisted or suspended, can't be registered")
    }

  }

  /**
    * The list of failed workers collected by failure detector
    * @return list of worker reference, worker ID and repair strategy
    */
  def failedWorkers: List[((ActorRef, WorkerID), RepairStrategy)] = failureDetector.failedWorkers

  /**
    * The list of workers suspected to be in failure state collected by failure detector
    * @return list of worker reference, worker ID and repair strategy
    */
  def suspectedWorkers: List[ActorRef] = failureDetector.suspectedWorkers.map(_._1)

  /**
    * Worker error handler
    * @param worker worker reference
    * @param error [[WorkerError]]
    * @return option of [[JobDescription]] for future schedule
    */
  def onError(worker: ActorRef, error: WorkerError): Future[Option[JobDescription]] = {
    log.error(s"Error of worker ${error.workerID} ${error.message}")
    log.info(s"Removing worker ${error.workerID} due to error, rescheduling jobs...")
    deactivateWorker(worker, error.workerID)
  }

  /**
    * Update handler. Does all the tracking and checks if job execution was finished or failed
    * @param worker worker reference
    * @param jobReportUpdate [[JobReportUpdate]]
    */
  def onUpdate(worker: ActorRef, jobReportUpdate: JobReportUpdate): Future[Option[JobDescription]] = {
    stateKeeper.update(jobReportUpdate.jobID, jobReportUpdate.jobReport).onComplete({
      case Success(true) => // Success ignoring
      case Success(false) => // failure
        log.warn(s"Job ${jobReportUpdate.jobID} status can't be updated.")
      case Failure(error) => // failure
        log.error(s"Error updating job ${jobReportUpdate.jobID} status.", error)
    })
    loadBalancer.track(worker, jobReportUpdate.jobID, jobReportUpdate.jobReport)
    failureDetector.track(worker, jobReportUpdate.workerID, jobReportUpdate.jobID, jobReportUpdate.jobReport)

    onExecutionFinished(worker, jobReportUpdate)
  }

  /**
    * Job schedule handler, saves the job so that it could be rescheduled in case of worker failure
    * @param jobID [[JobID]]
    * @param worker worker reference
    */
  def onJobScheduled(jobID: JobID, worker: ActorRef): Unit = {
    val jobSet = workerToJobMap.getOrElse(worker, Set.empty)
    workerToJobMap += ((worker, jobSet + jobID))

    log.info(s"Job $jobID was scheduled successfully, total jobs $activeJobsCount")
  }

  /**
    * Schedule the job functionality. Ask load balancer for worker and return proper response
    * @param job [[JobDescription]]
    * @return [[ScheduleJobResult]]
    */
  def scheduleJob(job: JobDescription): Future[ScheduleJobResult] = {
    val promise = Promise[ScheduleJobResult]()

    @inline def schedule() = {
      loadBalancer.getWorker(job.`class`) match {
        case None =>
          log.warn(s"Can't schedule job ${job.id}, no workers available")
          if(!promise.isCompleted)
          promise.success(Refuse(job.id, Messages.NO_WORKERS))
        case Some(worker) =>
          worker ! StartJob(job)
          if(!promise.isCompleted)
          promise.success(RequestSent(job.id))

      }
    }

    // the result of save operation should not affect job scheduling
    val saveAndGetParentJobsFuture = stateKeeper
      .save(JobInfo(job))
      .flatMap(_ => getParentJobs(job))

    saveAndGetParentJobsFuture.onComplete({
      case Success(Nil) =>
        // scheduling job according to requested parallelism
        for(_ <- 0 to job.settings.parallelism) schedule()
      case Success(jobs) =>
        val jobIDs = jobs.map(_.getOrElse("not-found"))
        if(!promise.isCompleted) promise.success(Suspend(job.id))
        log.warn(s"Job ${job.id} is not allowed for execution, because it's dependent on ${jobIDs.mkString(", ")}")
      case _ => // failure
        log.warn(s"Job ${job.id} is not allowed for execution.")
    })

    promise.future
  }

  // deregister job, and reschedule if retry is needed
  private def onExecutionFinished(worker: ActorRef, jobReportUpdate: JobReportUpdate): Future[Option[JobDescription]] = {
    jobReportUpdate.jobReport.status match {
      case JobStatus.FINISHED | JobStatus.FAILED =>
        workerToJobMap.get(worker) match {
          case None =>
            log.error(s"Worker ${jobReportUpdate.workerID} wasn't registered")
            Future.successful(None)
          case Some(jobSet) =>
            workerToJobMap += ((worker, jobSet - jobReportUpdate.jobID))
            log.info(s"active jobs: $activeJobsCount, " +
              s"workers: ${workerToJobMap.size}, " +
              s"suspended: ${suspendedWorkerSet.size}, " +
              s"blacklisted: ${blackListedWorkerSet.size}")
            if(jobReportUpdate.jobReport.status == JobStatus.FAILED) {
              val descriptionPromise = Promise[Option[JobDescription]]()
              stateKeeper.getById(jobReportUpdate.jobID).onComplete({
                case Success(Some(jobInfo)) =>
                  if(jobInfo.reportHistory.count(_.status == JobStatus.FAILED) < jobInfo.description.settings.retries) {
                    log.info(s"Job ${jobReportUpdate.jobID} will be rescheduled")
                    descriptionPromise.success(Some(jobInfo.description))
                  } else {
                    log.info(s"Job ${jobReportUpdate.jobID} will not be rescheduled, max retries exceeded")
                    descriptionPromise.success(None)
                  }
                case Success(None) =>
                  log.warn(s"Job ${jobReportUpdate.jobID} can't be rescheduled. No records found")
                  descriptionPromise.success(None)
                case Failure(error) =>
                  // Note that DB retires or any state retrieve failures should be handled by state keeper, or
                  // underlying classes
                  log.warn(s"Job ${jobReportUpdate.jobID} can't be rescheduled due to error", error)
                  descriptionPromise.success(None)
              })
              descriptionPromise.future
            } else Future.successful(None)
        }
      case _ => // ignore
        Future.successful(None)
    }
  }

  // deactivate worker and reschedule jovs
  private def deactivateWorker(worker: ActorRef, workerID: WorkerID): Future[Option[JobDescription]] = {
    val promise = Promise[Option[JobDescription]]()
    workerToJobMap.get(worker) match {
      case None =>
        log.info(s"nothing to reschedule for worker $workerID")
      case Some(jobSet) =>
        log.info(s"rescheduling ${jobSet.size} jobs for worker $workerID")
        jobSet.foreach(jobId => {
          stateKeeper.getById(jobId).onComplete({
            case Success(Some(jobDesctiption)) =>
              // here we don't need to get response because no clients waiting for feed back and job will be
              // rescheduled later if no workers found.
              //scheduleJob(jobDesctiption.description)
              promise.success(Some(jobDesctiption.description))
            case Success(None) =>
              log.error(s"Lost job $jobId. No job record found after worker failed to complete in time.")
            case Failure(error) =>
              log.error(s"Lost job $jobId due to error", error)
          })
        })
    }

    workerToJobMap -= worker

    loadBalancer.removeWorker(worker)

    promise.future
  }

  private def getParentJobs(jobDescription: JobDescription): Future[List[Option[JobID]]] = {
    Future.traverse(jobDescription.settings.dependsOn){ id =>
      stateKeeper.getById(id)
    }.map(jobs =>
      // check if all the required jobs completed
      jobs.collect({
        case None => None
        case Some(jobInfo) if jobInfo.reportHistory.exists(_.status != JobStatus.FINISHED) =>
            Some(jobInfo.description.id)
      })
    )
  }
}
