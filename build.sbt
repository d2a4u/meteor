import sbt.Keys.organization
import sbt.addCompilerPlugin

val catsVersion = "2.2.0"
val http4sVersion = "0.21.7"
val fs2Version = "2.4.2"

lazy val dependencies = Seq(
  "org.typelevel" %% "cats-core" % catsVersion,
  "org.typelevel" %% "cats-effect" % catsVersion,
  "co.fs2" %% "fs2-core" % fs2Version,
  "software.amazon.awssdk" % "dynamodb" % "2.14.15"
)

lazy val testDependencies = Seq(
  "org.scalatest" %% "scalatest" % "3.2.0",
  "org.scalacheck" %% "scalacheck" % "1.14.3"
).map(_ % "it,test")

lazy val ItTest = config("it").extend(Test)

lazy val commonSettings = Seq(
  organization in ThisBuild := "meteor",
  scalaVersion := "2.13.3",
  crossScalaVersions := Seq("2.12.12", "2.13.3"),
  parallelExecution in Test := false,
  scalafmtOnCompile := true,
  licenses += ("MIT", url("http://opensource.org/licenses/MIT")),
  publishMavenStyle := true,
  bintrayRepository := "meteor",
  publishArtifact in Test := false,
  pomIncludeRepository := { _ => false },
  releaseCrossBuild := true,
  bintrayReleaseOnPublish := false,
  addCompilerPlugin(
    "org.typelevel" % "kind-projector" % "0.11.0" cross CrossVersion.full
  ),
  libraryDependencies ++= dependencies ++ testDependencies,
  scalacOptions := Seq(
    "-unchecked",
    "-deprecation",
    "-feature",
    "-encoding",
    "utf8",
    "-language:higherKinds"
  )
)

lazy val root = project
  .in(file("."))
  .settings(name := "meteor", commonSettings, noPublish)
  .aggregate(awssdk)

lazy val noPublish =
  Seq(publish := {}, publishLocal := {}, publishArtifact := false)

lazy val awssdk = project
  .in(file("awssdk"))
  .configs(ItTest)
  .settings(
    inConfig(ItTest)(Defaults.testSettings),
    testOptions in ItTest += Tests.Argument("-oD")
  )
  .settings(
    name := "meteor-awssdk",
    libraryDependencies ++= dependencies ++ testDependencies,
    scalacOptions in Test ~= filterConsoleScalacOptions,
    scalacOptions in Compile ~= filterConsoleScalacOptions,
    commonSettings
  )
