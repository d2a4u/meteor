import sbt.Keys.organization
import sbt.addCompilerPlugin

val catsVersion = "2.2.0"
val http4sVersion = "0.21.7"
val fs2Version = "2.4.2"

lazy val dependencies = Seq(
  "org.typelevel" %% "cats-core" % catsVersion,
  "org.typelevel" %% "cats-effect" % catsVersion,
  "co.fs2" %% "fs2-core" % fs2Version,
  "org.scala-lang.modules" %% "scala-collection-compat" % "2.2.0",
  "org.scala-lang.modules" %% "scala-java8-compat" % "0.9.1",
  "software.amazon.awssdk" % "dynamodb" % "2.14.15"
)

lazy val testDependencies = Seq(
  "org.scalatest" %% "scalatest" % "3.2.2",
  "org.scalacheck" %% "scalacheck" % "1.14.3",
  "org.scalatestplus" %% "scalacheck-1-14" % "3.2.2.0"
)

lazy val ItTest = config("it").extend(Test)

lazy val scalaVer = "2.13.3"

lazy val commonSettings = Seq(
  organization in ThisBuild := "meteor",
  scalaVersion := scalaVer,
  crossScalaVersions ++= Seq("2.12.12"),
  parallelExecution in Test := true,
  scalafmtOnCompile := true,
  licenses += ("MIT", url("http://opensource.org/licenses/MIT")),
  publishMavenStyle := true,
  bintrayRepository := "meteor",
  publishArtifact in Test := false,
  pomIncludeRepository := { _ => false },
  bintrayReleaseOnPublish := false,
  releaseEarlyWith := BintrayPublisher,
  releaseEarlyEnableSyncToMaven := false,
  releaseEarlyNoGpg := true,
  scmInfo := Some(
    ScmInfo(
      url("https://github.com/d2a4u/meteor"),
      "git@github.com:d2a4u/meteor.git"
    )
  ),
  developers := List(
    Developer(
      "d2a4u",
      "D A Khu",
      "d2a4u@users.noreply.github.com",
      url("https://github.com/d2a4u")
    )
  ),
  addCompilerPlugin(
    "org.typelevel" % "kind-projector" % "0.11.0" cross CrossVersion.full
  ),
  scalacOptions := Seq(
    "-unchecked",
    "-deprecation",
    "-feature",
    "-encoding",
    "utf8",
    "-language:higherKinds"
  ),
  scalacOptions in Test ~= filterConsoleScalacOptions,
  scalacOptions in Compile ~= filterConsoleScalacOptions
)

lazy val root = project
  .in(file("."))
  .settings(name := "meteor", commonSettings, noPublish)
  .dependsOn(
    awssdk % "compile->compile;test->test",
    scanamo % "compile->compile;test->test"
  )
  .aggregate(awssdk, scanamo)

lazy val noPublish =
  Seq(publish := {}, publishLocal := {}, publishArtifact := false)

lazy val awssdk = project
  .in(file("awssdk"))
  .configs(ItTest)
  .settings(
    inConfig(ItTest)(Defaults.testSettings),
    testOptions in ItTest += Tests.Argument(
      "-oD"
    ) // enabled time measurement for each test
  )
  .settings(
    name := "meteor-awssdk",
    libraryDependencies ++= dependencies ++ testDependencies.map(_ % "it,test"),
    commonSettings
  )

lazy val scanamo = project
  .in(file("scanamo"))
  .settings(
    inConfig(Test)(Defaults.testSettings),
    testOptions in Test += Tests.Argument(
      "-oD"
    ) // enabled time measurement for each test
  )
  .settings(
    name := "meteor-scanamo",
    libraryDependencies ++= dependencies ++ testDependencies.map(
      _ % "test"
    ) ++ Seq(
      "org.scanamo" %% "scanamo-formats" % "1.0.0-M11"
    ),
    commonSettings
  ).dependsOn(awssdk)
