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