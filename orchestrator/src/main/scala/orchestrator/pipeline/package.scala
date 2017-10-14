package orchestrator

package object pipeline {

  /* Unique identifier for the Job */
  type JobID = String

  /* Name of the Job Class */
  type JobClass = String

  type JobWeight = Double

  type JobStatus = JobStatus.Value

  type Payload = Array[Byte]

  type JobCallback = (JobID, JobReport) => Unit

  type WorkerID = Int

}
