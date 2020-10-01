val catsVersion = "2.2.0"
val http4sVersion = "0.21.7"

lazy val dependencies = Seq(
  "org.typelevel" %% "cats-core" % catsVersion,
  "org.typelevel" %% "cats-effect" % catsVersion,
  "software.amazon.awssdk" % "dynamodb" % "2.14.15"
)

lazy val testDependencies = Seq(
  "org.scalatest" %% "scalatest" % "3.2.0",
  "org.scalacheck" %% "scalacheck" % "1.14.3"
).map(_ % "it,test")

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
  .settings(name := "meteor", commonSettings)
