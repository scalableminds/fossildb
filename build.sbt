import sbt._

name := "fossildb"

def getVersionFromGit: String = {
  def run(cmd: String): String = new java.io.BufferedReader(new java.io.InputStreamReader(java.lang.Runtime.getRuntime.exec(cmd).getInputStream)).readLine()
  def getBranch = run("git rev-parse --abbrev-ref HEAD")

  if (sys.env.contains("CI") && getBranch == "master") {
    val oldVersion = run("git describe --tags --abbrev=0").split('.').toList.map(_.toInt)
    (oldVersion.init :+ (oldVersion.last + 1)).mkString(".")
  } else {
    "DEV-" + getBranch
  }
}

ThisBuild / scalacOptions ++= Seq(
  "-feature",
  "-deprecation"
)

version := getVersionFromGit

scalaVersion := "2.13.12"

libraryDependencies ++= Seq(
  "ch.qos.logback" % "logback-classic" % "1.4.7",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",
  "org.scalatest" % "scalatest_2.13" % "3.2.15" % "test",
  "io.grpc" % "grpc-netty" % scalapb.compiler.Version.grpcJavaVersion,
  "io.grpc" % "grpc-services" % scalapb.compiler.Version.grpcJavaVersion,
  "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapb.compiler.Version.scalapbVersion,
  "org.rocksdb" % "rocksdbjni" % "7.10.2",
  "com.github.scopt" %% "scopt" % "4.1.0"
)

Compile / managedSourceDirectories += target.value / "protobuf-generated"

Compile / PB.targets := Seq(
  scalapb.gen() -> (target.value / "protobuf-generated")
)

Compile / mainClass := Some("com.scalableminds.fossildb.FossilDB")

assembly / assemblyMergeStrategy := {
  case x if x.endsWith("io.netty.versions.properties") => MergeStrategy.first
  case x =>
    val oldStrategy = (assembly / assemblyMergeStrategy).value
    oldStrategy(x)
}

assembly / assemblyJarName := "fossildb.jar"


lazy val buildInfoSettings = Seq(
  buildInfoKeys := Seq[BuildInfoKey](version,
    "commitHash" -> new java.lang.Object() {
      override def toString: String = {
        try {
          val extracted = new java.io.InputStreamReader(java.lang.Runtime.getRuntime.exec("git rev-parse HEAD").getInputStream)
          new java.io.BufferedReader(extracted).readLine()
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
