package orchestrator.pipeline

import akka.actor.ActorRef
import orchestrator.util.Loggable

import scala.collection.mutable
import scala.collection.concurrent.{TrieMap, Map => CMap}

/**
  * Master state is responsible of keeping the set of available workers being able to select the best worker for
  * the job, based on declared [[JobWeight]] and history of scheduled executions
  */
trait LoadBalancer {

  /**
    * Register worker with list of executable job classes [[JobClass]] with [[JobWeight]] declared by worker
    * @param worker worker reference
    * @param executableJobs list of executable jobs with declared wieghts
    */
  def registerWorker(worker: ActorRef, executableJobs: List[(JobClass, JobWeight)]): Unit

  /**
    * Get worker by [[JobClass]]. Master can implement different strategy to select worker, based on declared weight
    * and previous executions. On another hand workers could be selected in round robin fashion
    * @param jobClass job class
    * @return worker reference [[ActorRef]]
    */
  def getWorker(jobClass: JobClass): Option[ActorRef]

  /**
    * Remove worker from list of available worker to prevent job scheduling
    * @param worker
    */
  def removeWorker(worker: ActorRef): Unit

  /**
    * Remove worker from list of available worker to prevent job scheduling, but keep all the registered data
    * @param worker ActorRef
    */
  def suspendWorker(worker: ActorRef): Unit

  /**
    * Bring back worker to list of available worker to enable job scheduling
    * @param worker ActorRef
    */
  def activateWorker(worker: ActorRef): Unit

  /**
    * Track the job execution.
    * @param worker worker reference
    * @param jobID job ID
    * @param jobReport report
    */
  def track(worker: ActorRef, jobID: JobID, jobReport: JobReport): Unit
}

/**
  * Master that is responsible for job orchestration. Master class is not thread safe
  */
object RoundRobinLoadBalancer extends LoadBalancer with Loggable {

  private var workerQueueMap: CMap[JobClass, mutable.Queue[ActorRef]] = TrieMap.empty
  private var workerToClassMap: CMap[ActorRef, List[JobClass]] =TrieMap.empty

  def registerWorker(worker: ActorRef, executableJobs: List[(JobClass, JobWeight)]) = {
    // taking job classes only
    val jobClasses = executableJobs.map(_._1)

    log.info(s"removing worker for classes ${jobClasses.mkString(", ")}")

    // registering worker with it's classes
    workerToClassMap += ((worker, jobClasses))

    // registering worker per class

      jobClasses.foreach { cls =>
        val queue = workerQueueMap.getOrElse(cls, mutable.Queue.empty[ActorRef])
        queue.synchronized(queue.enqueue(worker))
        workerQueueMap += ((cls, queue))
      }
  }

  def getWorker(jobClass: JobClass): Option[ActorRef] = {
    workerQueueMap.get(jobClass).flatMap { queue =>
      queue.synchronized {
        if (queue.nonEmpty) {
          // round robin worker retrieval
          val worker = queue.dequeue()
          queue.enqueue(worker)
          Some(worker)
        } else None
      }
    }
  }

  def removeWorker(worker: ActorRef): Unit = {
    workerToClassMap.get(worker) match {
      case None =>
        log.error("tried to remove unregistered worker")
      case Some(jobClasses) =>
        log.info(s"removing worker for classes ${jobClasses.mkString(", ")}")
        for {
          cls <- jobClasses
          queue <- workerQueueMap.get(cls)
        } queue.synchronized(queue.dequeueAll(_ == worker))
    }
  }

  def track(worker: ActorRef, jobID: JobID, jobReport: JobReport): Unit = {}

  /**
    * Remove worker from list of available worker to prevent job scheduling, but keep all the registered data
    *
    * @param worker ActorRef
    */
  def suspendWorker(worker: ActorRef): Unit = ???

  /**
    * Bring back worker to list of available worker to enable job scheduling
    *
    * @param worker ActorRef
    */
  def activateWorker(worker: ActorRef): Unit = ???
}
