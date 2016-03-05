name := "spark-es-writer"

version := "0.1.0"

scalaVersion := "2.11.7"

val sparkVersion = "1.6.0"

javacOptions ++= Seq("-source", "1.7", "-target", "1.7")

scalacOptions += "-target:jvm-1.7"

val esVersion = "2.2.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",

  "com.sksamuel.elastic4s" %% "elastic4s-core" % esVersion,

  // TEST
  "org.scalatest" %% "scalatest" % "2.2.6" % "test"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => { MergeStrategy.first }
}