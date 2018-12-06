import sbtbuildinfo.BuildInfoKeys.{buildInfoKeys, buildInfoOptions, buildInfoPackage}
import sbtbuildinfo.{BuildInfoKey, BuildInfoOption}

name := "fossildb"

def getVersionFromGit: String = {
  def run(cmd: String): String = (new java.io.BufferedReader(new java.io.InputStreamReader(java.lang.Runtime.getRuntime().exec(cmd).getInputStream()))).readLine()
  def getBranch = run("git rev-parse --abbrev-ref HEAD")

  if (sys.env.get("CI").isDefined && getBranch == "master") {
    val oldVersion = run("git describe --tags --abbrev=0").split('.').toList.map(_.toInt)
    (oldVersion.init :+ (oldVersion.last + 1)).mkString(".")
  } else {
    "DEV-" + getBranch
  }
}

version := getVersionFromGit

scalaVersion := "2.12.4"

libraryDependencies ++= Seq(
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2",
  "org.scalatest" % "scalatest_2.12" % "3.0.4" % "test",
  "io.grpc" % "grpc-netty" % scalapb.compiler.Version.grpcJavaVersion,
  "io.grpc" % "grpc-services" % scalapb.compiler.Version.grpcJavaVersion,
  "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapb.compiler.Version.scalapbVersion,
  "org.rocksdb" % "rocksdbjni" % "5.17.2",
  "com.github.scopt" %% "scopt" % "3.7.0"
)

managedSourceDirectories in Compile += target.value / "protobuf-generated"

PB.targets in Compile := Seq(
  scalapb.gen() -> (target.value / "protobuf-generated")
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