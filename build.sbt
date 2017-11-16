import sbtbuildinfo.BuildInfoKeys.{buildInfoKeys, buildInfoOptions, buildInfoPackage}
import sbtbuildinfo.{BuildInfoKey, BuildInfoOption}

name := "fossildb"

version := "0.1"

scalaVersion := "2.12.4"

libraryDependencies ++= Seq(
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2",
  "org.scalatest" % "scalatest_2.12" % "3.0.4" % "test",
  "io.grpc" % "grpc-netty" % com.trueaccord.scalapb.compiler.Version.grpcJavaVersion,
  "com.trueaccord.scalapb" %% "scalapb-runtime-grpc" % com.trueaccord.scalapb.compiler.Version.scalapbVersion,
  "org.rocksdb" % "rocksdbjni" % "5.1.2",
  "com.github.scopt" %% "scopt" % "3.7.0"
)

PB.targets in Compile := Seq(
  scalapb.gen() -> (sourceManaged in Compile).value
)

mainClass in Compile := Some("com.scalableminds.fossildb.FossilDB")

assemblyMergeStrategy in assembly := {
  case x if x.endsWith("io.netty.versions.properties") => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

assemblyJarName in assembly := "fossildb.jar"


lazy val buildInfoSettings = Seq(
  buildInfoKeys := Seq[BuildInfoKey](version,
    "commitHash" -> new java.lang.Object() {
      override def toString(): String = {
        try {
          val extracted = new java.io.InputStreamReader(java.lang.Runtime.getRuntime().exec("git rev-parse HEAD").getInputStream())
          (new java.io.BufferedReader(extracted)).readLine()
        } catch {
          case t: Throwable => "get git hash failed"
        }
      }
    }.toString()
  ),
  buildInfoPackage := "fossildb",
  buildInfoOptions := Seq(BuildInfoOption.BuildTime, BuildInfoOption.ToJson)
)

lazy val root = (project in file(".")).
  enablePlugins(BuildInfoPlugin).
  settings(
    buildInfoSettings
  )