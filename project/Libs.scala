package builds

import sbt._

object Libs {

  val akkaVersion = "2.5.3"

  val akka_actor        = "com.typesafe.akka"       %% "akka-actor"         % akkaVersion
  val akka_logger       = "com.typesafe.akka"       %% "akka-slf4j"         % akkaVersion
  val akka_testkit      = "com.typesafe.akka"       %% "akka-testkit"       % akkaVersion

  val joda_time         = "joda-time"                % "joda-time"          % "2.9.9"
  val google_guice      = "com.google.inject"        % "guice"              % "4.0"
  val scallop           = "org.rogach"              %% "scallop"            % "3.1.0"
  val logback           = "ch.qos.logback"           % "logback-classic"    % "1.2.3"
  val scala_test        = "org.scalatest"           %% "scalatest"          % "3.0.1"       % Test

}
