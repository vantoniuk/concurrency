package com.leapfin.task


import com.google.inject.{Guice, Injector}
import com.leapfin.task.util.Loggable
import org.joda.time.DateTime
import org.rogach.scallop.ScallopConf

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.util.{Failure, Success}
import scala.concurrent.duration._

class StringLookupConf(arguments: Seq[String]) extends ScallopConf(arguments) with Loggable {
  private val timeoutRegex = "(\\d+)(\\w+)".r
  private def parseTimeout(strTimeout: String): FiniteDuration = strTimeout match {
    case timeoutRegex(value, unit) => FiniteDuration(value.toLong, unit.toLowerCase())
    case _ =>
      log.error(s"Error, can't parse timeout $strTimeout")
      throw new Error
  }

  val lookupString = opt[String](required = false, name = "lookup-string", descr = "String for lookup in random stream", default = Some("hello"))
  val totalWorkers = opt[Int](required = false, name = "workers", descr = "Number of parallel workers", default = Some(10))
  val timeout = opt[String](required = false, name = "timeout", descr = "Execution timeout for lookup process, e.g.: 10ms, 5sec, 3min etc.", default = Some("60sec")).map(parseTimeout)
  val shutDownTime = opt[String](required = false, name = "shut-down", descr = "Timeout system to shutdown necessary to guarantee SLA, e.g.: 10ms, 5sec, 3min etc.", default = Some("100ms")).map(parseTimeout)

  validate(timeout, shutDownTime) { (timeoutV, sdTimeV) =>
    if(timeoutV > sdTimeV) Right(Unit)
    else Left("Execution timeout should be bigger than shut down time")
  }
  
  verify()
}

object Main extends App with Loggable {
  private val conf = new StringLookupConf(args)
  private val injector: Injector = Guice.createInjector(new DefaultModule)

  private val stringLookup: StringLookup = injector.getInstance(classOf[StringLookup])
  private implicit val executionContext: ExecutionContext = injector.getInstance(classOf[ExecutionContext])

  private val startTime = DateTime.now

  stringLookup.lookup(
    conf.lookupString.toOption.get,
    conf.totalWorkers.toOption.get,
    conf.timeout.toOption.get.minus(conf.shutDownTime.toOption.get) // reduce timeout to save time for report
  ).onComplete({
    case Success(report) =>
      stringLookup.terminate()
      log.info(report)
      val executionTime = DateTime.now.getMillis - startTime.getMillis
      log.info(s"Execution time $executionTime[ms]")
    case Failure(error) =>
      log.error("Failed to generate report", error)
  })

}
