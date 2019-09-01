import sbt.Keys.publishArtifact

lazy val commonSettings = Seq(
  name := "metrics_analytics",
  version := "0.1",
  organization := "br.com.spark",
  scalaVersion := "2.11.0",
  test in assembly := {},
)

val sparkVersion = "2.3.2"

lazy val app = (project in file("."))
  .settings(commonSettings: _*)
  .settings(

    libraryDependencies ++= Seq(
      "org.scalactic" %% "scalactic" % "3.0.1",
      "org.scalatest" %% "scalatest" % "3.0.0" % "test",
      "org.scalamock" %% "scalamock-scalatest-support" % "3.5.0" % "test",
      "org.pegdown" % "pegdown" % "1.6.0" % "test",
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
      , "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
      , "org.apache.spark" %% "spark-hive" % sparkVersion % "provided"
      , "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided" 
      , "com.typesafe" % "config" % "1.3.3"
      , "com.holdenkarau" %% "spark-testing-base" % "2.3.2_0.11.0" % Test
    ),

    publishArtifact in Test := false,

    parallelExecution in Test := false,

    scapegoatVersion in ThisBuild := "1.3.6",

    testOptions in Test ++= Seq(
      Tests.Argument(TestFrameworks.ScalaTest, "-o"),
      Tests.Argument(TestFrameworks.ScalaTest, "-h", "smtx-s8-book_client-view_metrics/spark/target/test-reports")
    ),

    coverageMinimum := 50,

    coverageFailOnMinimum := true,

    sonarProperties := Map(
      "sonar.projectName" -> "spark-analytics-set",
      "sonar.projectKey" -> "spark-analytics-set",
      "sonar.sources" -> "smtx-s8-book_client-view_metrics/spark/src/main/scala",
      "sonar.tests" -> "smtx-s8-book_client-view_metrics/spark/src/test/scala",
      "sonar.surefire.reportsPath" -> "smtx-s8-book_client-view_metrics/spark/target/test-reports",
      "sonar.junit.reportPaths" -> "smtx-s8-book_client-view_metrics/spark/target/test-reports",
      "sonar.sourceEncoding" -> "UTF-8",
      "sonar.scala.coverage.reportPaths" -> "smtx-s8-book_client-view_metrics/spark/target/scala-2.11/scoverage-report/scoverage.xml",
      "sonar.scala.scapegoat.reportPaths" -> "smtx-s8-book_client-view_metrics/spark/target/scala-2.11/scapegoat-report/scapegoat.xml",
      "sonar.host.url" -> "http://sonarqube.server.com:19000"
    ),
    
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", xs@_*) => MergeStrategy.discard
      case PathList("javax", "servlet", xs@_*) => MergeStrategy.first
      case PathList(ps@_*) if ps.last endsWith ".html" => MergeStrategy.first
      case "application.conf" => MergeStrategy.concat
      case "unwanted.txt" => MergeStrategy.discard
      case x => MergeStrategy.first
    }
  
    , assemblyShadeRules in assembly ++= Seq(
        ShadeRule.rename("org.apache.http.**" -> "other.org.apache.http.@1")
        .inLibrary("org.apache.httpcomponents" % "httpclient" % "4.3.5")
        .inProject
    )

    , parallelExecution in Test := false
    , fork in Test := true
    , javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")
  )

