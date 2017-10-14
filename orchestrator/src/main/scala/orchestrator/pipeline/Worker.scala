package orchestrator.pipeline

import orchestrator.pipeline.Messages._
import orchestrator.util.Loggable

import scala.concurrent.Future
import scala.collection.concurrent.{Map => CMap, TrieMap}

/**
  * Worker interface. Potentially worker by any remove service which register itself as capable of
  * executing tasks of certain class. Worker interface plays facade role in case if remote service uses
  * different format of messages / protocol for communication.
  *
  * General guide line for remove workers is to be able to response to execute, get executable job classes
  * and get report calls including health check which is [[Messages.HEALTH_CHECK_ID]] value.
  *
  * Updates can be detected using get report call or using server push mechanism.
  *
  * Remote worker can implement get report call such that it will respond immediately for health check
  * if some parameter provided or certain endpoint invoked. And it can delay response until any updates occur
  * in long poll fashion otherwise.
  */
trait Worker {
  /**
    * Unique worker identifier
    * @return
    */
  def id: WorkerID

  /**
    * Register job method
    * @param jobDescription description including JobID and Payload
    * @param updateCallback on update callback used to send all the updates in job progress.
    *                 Callback should be invoked to ensure the job health
    * @return
    */
  def execute(jobDescription: JobDescription, updateCallback: (JobID, JobReport) => Unit): Future[ScheduleJobResult]

  /**
    * Get job report. The method is used to get the progress update in pull fashion. In addition can be used
    * to ensure worker/job health
    * @param jobID job ID
    * @return [[JobReport]]
    */
  def getReport(jobID: JobID): Future[Option[JobReport]]

  /**
    * get all executable job classes. Executable [[JobClass]] gives master the information about jobs that can
    * be executed on such worker
    * @return
    */
  def executableJobClasses: Future[List[(JobClass, JobWeight)]]
}


/**
  * Job Handler
  * @param executorFactory factory for local job executor
  * @param weight job weight, to keep track of worker capacity
  */
case class JobHandler(executorFactory: JobExecutorFactory, weight: JobWeight)

/**
  * Local Worker. Worker class is not thread safe. Should be used within worker Actor
  * @param handlers Map from [[JobClass]] to [[JobHandler]]
  */
class LocalWorker(handlers: Map[JobClass, JobHandler], capacity: Double, val id: WorkerID) extends Worker with Loggable {
  private var jobRegistry: Map[JobID, JobExecutor] = Map.empty
  private var jobProgress: CMap[JobID, JobReport] = TrieMap.empty[JobID, JobReport]
  private var currentCapacity = capacity

  /**
    * Register job method
    *
    * @param jobDescription description including JobID and Payload
    * @param jobCallback on update callback used to send all the updates in job progress.
    *                       Callback should be invoked to ensure the job health
    * @return
    */
  def execute(jobDescription: JobDescription, jobCallback: JobCallback): Future[ScheduleJobResult] = {
//    log.info(s"local worker: $id receiver request ${jobDescription.id} ${jobDescription.`class`}")
    handlers.get(jobDescription.`class`) match {
      case Some(handler) if handler.weight <= currentCapacity =>
        log.info(s"local worker $id accepted ${jobDescription.id} ${jobDescription.`class`}")
        registerJob(jobDescription.id, jobDescription.payload, handler, jobCallback)
        Future.successful(Ack(jobDescription.id))
      case Some(_) =>
        log.warn(s"local worker $id ${Messages.NOT_ENOUGH_RESOURCES} ${jobDescription.`class`}")
        Future.successful(Refuse(jobDescription.id, Messages.NOT_ENOUGH_RESOURCES))

      case None =>
        log.warn(s"local worker $id ${Messages.CLASS_NOT_SUPPORTED} ${jobDescription.`class`}")
        Future.successful(Refuse(jobDescription.id, Messages.CLASS_NOT_SUPPORTED))
    }
  }

  /**
    * Get job report. The method is used to get the progress update in pull fashion. In addition can be used
    * to ensure worker/job health
    *
    * @param jobID job ID
    * @return [[JobReport]]
    */
  def getReport(jobID: JobID): Future[Option[JobReport]] = {
    if(jobID == Messages.HEALTH_CHECK_ID) {
      Future.successful(Some(JobReport.generate))
    } else Future.successful(jobProgress.get(jobID))
  }

  /**
    * Enumerate all the job classes that can be executed on this worker
    * @return list of tuples [[JobClass]] and [[JobWeight]]
    */
  def executableJobClasses: Future[List[(JobClass, JobWeight)]] = {
    val result = handlers.toList.map({case (cls, handler) => cls -> handler.weight})

    Future.successful(result)
  }

  private def registerJob(jobID: JobID, payload: Payload, handler: JobHandler, jobCallback: JobCallback): Unit = {
    val executor = handler.executorFactory.create(jobID)
    // register handler for JobID
    jobRegistry += ((jobID, executor))
    currentCapacity -= handler.weight

    executor.execute(payload, (_jobID, _report) => {
      log.info(s"local worker $id reports ${_jobID} ${_report}")
      _report.status match {
        case JobStatus.FINISHED | JobStatus.FAILED | JobStatus.UNKNOWN =>
          // deregister job
          jobProgress -= _jobID
          jobRegistry -= _jobID
          currentCapacity += handler.weight
          log.info(s"local worker $id finished the job ${_jobID}")
        case _ =>
          jobProgress += (_jobID -> _report)
      }
      jobCallback(_jobID, _report)
    })
  }

}
