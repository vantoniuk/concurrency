package orchestrator

import akka.actor.{ActorRef, ActorSystem, Props}
import com.google.inject.{Guice, Injector}
import orchestrator.Main.{handlers, jobClasses}
import orchestrator.di.DefaultModule
import orchestrator.pipeline.Messages.{ScheduleJob, ScheduleJobResult}
import orchestrator.pipeline._
import orchestrator.pipeline.actor.{MasterActor, WorkerActor}
import orchestrator.util.Loggable
import org.joda.time.DateTime
import org.rogach.scallop.ScallopConf

import scala.concurrent.{ExecutionContext, Promise}
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Random, Success}
import scala.concurrent.duration._

class OrchestratorConf(arguments: Seq[String]) extends ScallopConf(arguments) with Loggable {
  private val timeoutRegex = "(\\d+)(\\w+)".r
  private def parseTimeout(strTimeout: String): FiniteDuration = strTimeout match {
    case timeoutRegex(value, unit) => FiniteDuration(value.toLong, unit.toLowerCase())
    case _ =>
      log.error(s"Error, can't parse timeout $strTimeout")
      throw new Error
  }

  val healthCheckInterval = opt[String](required = false, name = "health-check", descr = "Interval between health check e.g.: 10ms, 5sec, 3min etc.", default = Some("2sec")).map(parseTimeout)
  val workersToCheck = opt[Int](required = false, name = "workers-check", descr = "Number of workers to check during health check", default = Some(2))

  val retryInterval = opt[String](required = false, name = "retry-interval", descr = "Interval between job retries e.g.: 10ms, 5sec, 3min etc.", default = Some("10sec")).map(parseTimeout)
  val jobToRetry = opt[Int](required = false, name = "jobs-retry", descr = "Number of jobs eligible for retry", default = Some(2))

  
  verify()
}

object Main extends App with Loggable {
  private val conf = new OrchestratorConf(args)

  private val injector: Injector = Guice.createInjector(new DefaultModule)

  private val master: Master = injector.getInstance(classOf[Master])

  private val actorSystem: ActorSystem = injector.getInstance(classOf[ActorSystem])

  private implicit val ctx: ExecutionContext = actorSystem.dispatcher

  val masterActor = actorSystem.actorOf(
    Props(classOf[MasterActor],
    master,
    conf.workersToCheck.toOption.get,
    conf.healthCheckInterval.toOption.get,
    conf.jobToRetry.toOption.get,
    conf.retryInterval.toOption.get
  ))

  // job classes for testing
  val jobClasses = List("cls1", "cls2", "cls3", "cls4", "cls5")

  // creating handlers for testing
  val handlers = List(
    createHandler(10, 2 seconds, 30 seconds, 30D),
    createHandler(10, 2 seconds, 15 seconds, 15D),
    createHandler(5, 2 seconds, 7500 millis, 7.5D),
    createHandler(12, 2 seconds, 12000 millis, 12D))

  // creating workers
  val workers = for(id <- 0 to 5) yield createWorker(jobClasses, handlers, 100D * Random.nextDouble() + 15D, id)

  for (jobID <- 0 to 100) {
    val dependentJobs = if(Random.nextDouble() > 0.9) List(Random.nextInt(100).toString) else Nil
    val executionInterval = if(Random.nextDouble() > 0.9) Some(10 seconds) else None
    val jobSettings = JobSettings(1, 2, dependentJobs, None, executionInterval)
    val jobDescription = JobDescription(
      jobID.toString,
      Random.shuffle(jobClasses).head,
      Array.empty,
      jobSettings
    )

    val promise = Promise[ScheduleJobResult]()
    masterActor ! ScheduleJob(jobDescription, promise)
    promise.future.foreach({
      scheduleResult => log.info(s"==============> $scheduleResult")
    })
  }

  private def createHandler(stages: Int,
                            reportDelay: FiniteDuration,
                            execution: FiniteDuration,
                            weight: JobWeight): JobHandler = {
    JobHandler(
      // starting from java 8 we can use function instead of interface with single method
      JobExecutor.dummyExecutor(stages, reportDelay, execution, actorSystem),
      weight
    )
  }

  private def createWorker(jobClasses: List[JobClass], handlers: List[JobHandler], capacity: Double, id: WorkerID): ActorRef = {
    val handlerMap = (for {
      cls <- Random.shuffle(jobClasses)
      handler <- Random.shuffle(handlers) if Random.nextDouble() > 0.7
    } yield cls -> handler).toMap

    val worker = new LocalWorker(handlerMap, capacity, id)
    actorSystem.actorOf(Props(classOf[WorkerActor], worker, masterActor))
  }

}
