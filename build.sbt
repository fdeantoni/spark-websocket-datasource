import Dependencies._

ThisBuild / scalaVersion     := "2.11.12"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"

lazy val root = (project in file("."))
  .settings(
    name := "Spark Websocket Datasource",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "2.4.4" % "provided",
      "org.apache.spark" %% "spark-streaming" % "2.4.4" % "provided",
      "com.squareup.okhttp3" % "okhttp" % "4.2.0",
      "com.typesafe.akka" %% "akka-stream" % "2.5.25" % Test,
      "com.typesafe.akka" %% "akka-slf4j" % "2.5.25" % Test,
      "com.typesafe.akka" %% "akka-testkit" % "2.5.25" % Test,
      "com.typesafe.akka" %% "akka-http" % "10.1.10" % Test,
      scalaTest % Test,
      "org.clapper" %% "grizzled-slf4j" % "1.3.1" % Test,
      "ch.qos.logback" % "logback-classic" % "1.2.3" % Test
    )
  )

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
