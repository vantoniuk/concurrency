import builds.Libs

name := "va-concurrency"

lazy val commonSettings = Seq(
  version := "1.0",
  scalaVersion := "2.12.3",
  resolvers ++= commonResolvers
)

lazy val commonResolvers = Seq(
  "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
  "Sonatype release Repository" at "http://oss.sonatype.org/service/local/staging/deploy/maven2/"
)

lazy val root = (project in file("."))
  .aggregate(orchestrator, monkey_typist)

lazy val orchestrator = (project in file("orchestrator"))
  .settings(name := "orchestrator")
  .settings(commonSettings:_*)
  .settings(libraryDependencies ++= Seq(
    Libs.akka_actor,
    Libs.akka_logger,
    Libs.akka_testkit,
    Libs.joda_time,
    Libs.google_guice,
    Libs.scallop,
    Libs.logback,
    Libs.scala_test
  ))

lazy val monkey_typist = (project in file("monkey_typist"))
  .settings(name := "monkey_typist")
  .settings(commonSettings:_*)
  .settings(libraryDependencies ++= Seq(
    Libs.akka_actor,
    Libs.akka_logger,
    Libs.akka_testkit,
    Libs.joda_time,
    Libs.google_guice,
    Libs.scallop,
    Libs.logback,
    Libs.scala_test
  ))
