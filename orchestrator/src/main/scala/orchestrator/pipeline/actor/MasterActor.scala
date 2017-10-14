package orchestrator.pipeline.actor

import orchestrator.pipeline.Messages._
import orchestrator.pipeline._
import org.joda.time.DateTime

import scala.concurrent.{ExecutionContext, Promise}
import scala.concurrent.duration.FiniteDuration
import scala.util.{Random, Success}
import scala.concurrent.duration._

class MasterActor(master: Master,
                  workersToCheck: Int = 2,
                  healthCheckInterval: FiniteDuration = 2 seconds,
                  jobsToRetry: Int = 2,
                  retryInterval: FiniteDuration = 10 seconds) extends IgnoreStateActor {
  private implicit val ctx: ExecutionContext = context.dispatcher

  def receive: Receive = process orElse ignore

  private var pendingJobs: Map[JobID, (JobDescription, DateTime, Promise[ScheduleJobResult])] = Map.empty

  private var intervalBasedJobMap: Map[JobDescription, DateTime] = Map.empty

  // go through pending jobs and reschedule if current time - schedule time is more that retryInterval
  context.system.scheduler.schedule(retryInterval / 2, retryInterval, self, RescheduleIntervalJobs)

  // go through pending jobs and reschedule if current time - schedule time is more that retryInterval
  context.system.scheduler.schedule(retryInterval, retryInterval, self, ReschedulePendingJobs)

  // check worker health
  context.system.scheduler.schedule(healthCheckInterval, healthCheckInterval, self, WorkerHealthCheck)

  private def process: Receive = {

    case RegisterExecutables(workerID, jobClasses) =>
      master.registerWorker(sender, workerID, jobClasses)

    // if this job should be scheduled based on interval and is not registered yet
    case ScheduleJob(jobDescription: JobDescription, promise) if
                      jobDescription.settings.executionInterval.isDefined &&
                      !intervalBasedJobMap.contains(jobDescription) =>
      if(intervalBasedJobMap.size < master.intervalJobLimit) {
        log.info(s"Job ${jobDescription.id} is registered for later schedule")
        intervalBasedJobMap += ((jobDescription, DateTime.now))
        promise.success(Ack(jobDescription.id))
      } else {
        log.info(s"Job ${jobDescription.id} is not registered for later schedule, no free slots lef")
        promise.success(Refuse(jobDescription.id, Messages.NO_FREE_SLOTS))
      }

    case ScheduleJob(jobDescription: JobDescription, promise) if pendingJobs.size == master.pendingJobLimit =>
      log.error(s"Can't schedule new job, system at capacity")
      promise.success(Refuse(jobDescription.id, Messages.NOT_ENOUGH_RESOURCES))

    case ScheduleJob(jobDescription: JobDescription, promise) =>
      pendingJobs += ((jobDescription.id, (jobDescription, DateTime.now, promise)))
      master.scheduleJob(jobDescription)

    case Ack(jobId) =>
      master.onJobScheduled(jobId, sender)
      withPendingJob(jobId)({
        case (_, _, promise) =>
          pendingJobs -= jobId
          promise.success(Ack(jobId))
      })
    case Refuse(_, Messages.NO_WORKERS) if pendingJobs.size < master.pendingJobLimit =>
      // keep in pending, so that it will be rescheduled
    case Refuse(_, Messages.NOT_ENOUGH_RESOURCES) if pendingJobs.size < master.pendingJobLimit =>
      // keep in pending, so that it will be rescheduled
    case refuse @ Refuse(jobId, _) =>
      withPendingJob(jobId)({case (_, _, promise) =>
        pendingJobs -= jobId
        promise.success(refuse)
      })

    case Suspend(jobId) =>
      pendingJobs.get(jobId) match {
        case None =>
          log.error(s"Job $jobId not found by id")
        case Some((jobDescription, _, promise)) =>
          // return jobs to suspended, but resetting the timer
          pendingJobs += ((jobId, (jobDescription, DateTime.now, promise)))
      }

    case RequestSent(_) =>
      // ignoring for now, message was sent to worker for execution, waiting for worker to respond

    case workerError @ WorkerError(_, _) =>
      master.onError(sender, workerError).foreach({
        case Some(jobToSchedule) =>
          self ! ScheduleJob(jobToSchedule)
        case None => // ignore
      })
    case update @ JobReportUpdate(_, _, _) =>
      master.onUpdate(sender, update).foreach({
        case Some(jobToSchedule) =>
          self ! ScheduleJob(jobToSchedule)
        case None => // ignore
      })

    case RescheduleIntervalJobs =>
      val currentTimeInMillis = DateTime.now.getMillis

      for {
        (description, timestamp) <- intervalBasedJobMap
        interval <- description.settings.executionInterval
      } {
        val millisSinceLastSchedule = currentTimeInMillis - timestamp.getMillis
        // check if job should be scheduled within next before next ResheduleIntervalJobs check
        if(millisSinceLastSchedule + retryInterval.toMillis > interval.toMillis) {
          log.info(s"Rescheduling ${description.id} as interval based job")
          val delay = (interval.toMillis - millisSinceLastSchedule).millis

          context.system.scheduler.scheduleOnce(delay, self, ScheduleJob(description))
        }
      }
      
    case ReschedulePendingJobs =>
      val currentTimeInMillis = DateTime.now.getMillis
      val (intervalBasedPendingJobs, otherPendingJobs) = pendingJobs.values.partition({
        case (desctiption, _, _) => desctiption.settings.executionInterval.isDefined
      })

      val eligiblePendingJobs = otherPendingJobs
        .filter({case (_, timestamp, _) => currentTimeInMillis - timestamp.getMillis > retryInterval.toMillis})
        .toList

      val eligibleJobsToRetryCount = (jobsToRetry - intervalBasedPendingJobs.size) max 0

      val jobForReschedule =
        intervalBasedPendingJobs ++
        Random
        .shuffle(eligiblePendingJobs)
        .take(eligibleJobsToRetryCount)

      log.info(s"Rescheduling ${jobsToRetry.min(jobForReschedule.size)} pending jobs")

      jobForReschedule.foreach({case (jobDescription, _, promise) =>
        self ! ScheduleJob(jobDescription, promise)
      })

    case WorkerHealthCheck =>
      master.failedWorkers.foreach({
        case ((worker, workerID), BlackList) =>
          master.blackListWorker(worker, workerID).foreach({
            case Some(jobToSchedule) =>
              self ! ScheduleJob(jobToSchedule)
            case None => // ignore
          })
        case ((worker, workerID), AvoidInteraction(duration)) =>
          // suspend worker
          master.suspendWorker(worker, workerID).foreach({
            case Some(jobToSchedule) =>
              self ! ScheduleJob(jobToSchedule)
            case None => // ignore
          })
          // schedule worker activation
          context.system.scheduler.scheduleOnce(duration, self, ActivateWorker(worker, workerID))
      })

      master.suspectedWorkers.foreach({worker => worker ! GetJobReport(Messages.HEALTH_CHECK_ID)})

    case ActivateWorker(worker, workerID) =>
      master.activateWorker(worker, workerID)

  }

  private def withPendingJob[T](jobId: JobID)(f: (JobDescription, DateTime, Promise[ScheduleJobResult]) => T): Option[T] = {
    pendingJobs.get(jobId) match {
      case None =>
        log.error(s"Job $jobId not found by id")
        None
      case Some((jobDescription, timestemp, promise)) =>
        Some(f(jobDescription, timestemp, promise))
    }
  }
}
