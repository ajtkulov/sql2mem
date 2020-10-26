name := "sql2mem"

version := "0.1"

parallelExecution in Test := false

fork := true

lazy val commonSettings = Seq(
  organization := "com.github.ajtkulov",
  scalaVersion := "2.12.10",
  sources in(Compile, doc) := Seq.empty,
  publishArtifact in(Compile, packageDoc) := false,
  publishArtifact in(Compile, packageSrc) := false
)

lazy val core = Project(id = "core", base = file("core"))
  .settings(commonSettings: _*)

lazy val root = Project(id = "sql2mem", base = file("."))
  .aggregate(core)
