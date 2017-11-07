name := "kvservice"

version := "1.0"

scalaVersion := "2.11.6"

libraryDependencies ++= Seq(
)

PB.targets in Compile := Seq(
  scalapb.gen() -> (sourceManaged in Compile).value
)

mainClass in Compile := Some("com.scalableminds.kvservice.Hi")
