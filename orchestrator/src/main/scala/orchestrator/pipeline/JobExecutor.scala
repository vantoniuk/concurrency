package orchestrator.pipeline

import akka.actor.ActorSystem

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.FiniteDuration
import akka.pattern.after
import org.joda.time.DateTime

/**
  * Executor interface for local jobs.
  */
trait JobExecutor {
  /**
    * Executes the job
    * @param payload job payload
    * @param callback callback to provide progress updates, when updates occur
    */
  def execute(payload: Payload, callback: JobCallback): Unit

  /**
    * Provides report for the job. General use case if to ensure worker health.
    * @return [[JobReport]]
    */
  def getReport: Future[JobReport]
}

object JobExecutor {
  def dummyExecutor(stages: Int,
                    reportDelay: FiniteDuration,
                    execution: FiniteDuration,
                    actorSystem: ActorSystem)(jobID: JobID): JobExecutor = new JobExecutor {
    private implicit val ctx: ExecutionContext = actorSystem.dispatcher
    private var timestamp: DateTime = DateTime.now
    private var progress = 0D
    private def currentState(p: Double): JobReport = {
      progress = p
      val status = if(p < 1) JobStatus.IN_PROGRESS else JobStatus.FINISHED

      JobReport(p, status, s"accepted $jobID", timestamp)
    }

    def execute(payload: Payload, callback: JobCallback): Unit = {
      timestamp = DateTime.now
      for(stage <- 0 to stages) {
        val scheduledFuture = after(execution / stages * stage, actorSystem.scheduler)(Future.successful(currentState(1D / stages * stage)))
        scheduledFuture.foreach(jobReport => callback(jobID, jobReport))
      }
    }

    def getReport: Future[JobReport] = {
      after(reportDelay, actorSystem.scheduler)(Future.successful(currentState(progress)))
    }
  }
}

trait JobExecutorFactory {
  def create(jobID: JobID): JobExecutor
}
