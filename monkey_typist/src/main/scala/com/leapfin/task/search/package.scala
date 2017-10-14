package com.leapfin.task

import scala.concurrent.Promise
import scala.concurrent.duration.FiniteDuration

package object search {
  /**
    * Value class for forker ID
    * @param id id value
    */
  case class WorkerId(id: Int) extends AnyVal {
    override def toString: String = id.toString
  }

  /**
    * Message to start lookup process
    * @param lookupString
    * @param totalWorkers
    * @param timeout
    * @param reportPromise
    */
  case class StartLookup(lookupString: String, totalWorkers: Int, timeout: FiniteDuration, reportPromise: Promise[MasterResponse])

  /**
    * Report content interface
    */
  sealed trait WorkerReportContent

  /**
    * Success report message
    * @param bytesRead number of bytes read
    * @param elapsedTime elapsed time in millis
    */
  case class SuccessWorkerReport(bytesRead: Long, elapsedTime: FiniteDuration) extends WorkerReportContent {
    override def toString: String = s"[${elapsedTime.toMillis}][$bytesRead][Success]"
  }

  /**
    * Failure report message
    * @param error error that occurred during process
    */
  case class FailureWorkerReport(error: Throwable) extends WorkerReportContent {
    override def toString: String = s"[][][Error]"
  }

  /**
    * Timeout report
    */
  case object TimeoutWorkerReport extends WorkerReportContent {
    override def toString: String = s"[][][Timeout]"
  }

  /**
    * Individual worker report message
    * @param workerId worker id
    * @param reportContent report content
    */
  case class WorkerReportMessage(workerId: WorkerId, reportContent: WorkerReportContent)

  sealed trait MasterResponse

  case class MasterReportResponse(workReport: Map[WorkerId, WorkerReportContent]) extends MasterResponse

  /**
    * Message that indicates that master is in progress
    */
  case object InProgressResponse extends MasterResponse
}
