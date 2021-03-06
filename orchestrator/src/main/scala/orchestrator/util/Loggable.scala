package orchestrator.util

import org.slf4j.{Logger, LoggerFactory}

trait Loggable {
  protected val log: Logger = LoggerFactory.getLogger(this.getClass)
}

