import Dependencies._

lazy val sparkVersion = "2.1.2"

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.telefonica",
      scalaVersion := "2.11.12",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "Spark Playpen",
    libraryDependencies += scalaTest % Test,
    libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion,
    libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion,
    libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion,
    libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
    libraryDependencies += "org.apache.spark" %% "spark-mllib" % sparkVersion,
    resolvers += Resolver.mavenLocal
  )
