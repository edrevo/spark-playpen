import Dependencies._

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "com.telefonica",
      scalaVersion := "2.12.7",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "Spark Playpen",
    libraryDependencies += scalaTest % Test,
    libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0",
    libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.0",
    libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.0",
    libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.0",
    resolvers += Resolver.mavenLocal
  )
