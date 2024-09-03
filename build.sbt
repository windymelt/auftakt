val scala3Version = "3.5.0"
val scribeVersion = "3.15.0"
val sttpVersion = "3.9.8"
val http4sVersion = "0.23.27"

ThisBuild / usePipelining := true

lazy val root = project
  .in(file("."))
  .settings(
    name         := "doobie exercise",
    version      := "0.1.0-SNAPSHOT",
    scalaVersion := scala3Version,
    scalacOptions := Seq(
      "-Wnonunit-statement",
      "-Ybest-effort",
      "-Ywith-best-effort-tasty",
    ),
    fork := true,
    usePipelining := true,
    libraryDependencies ++= Seq(
      // Start with this one
      "org.tpolecat" %% "doobie-core"     % "1.0.0-RC4",
      "org.tpolecat" %% "doobie-hikari"   % "1.0.0-RC4", // HikariCP transactor.
      "org.tpolecat" %% "doobie-postgres" % "1.0.0-RC4",
      "com.outr"     %% "scribe"          % scribeVersion,
      "com.outr"     %% "scribe-cats"     % scribeVersion,
      "com.softwaremill.sttp.client3" %% "core" % sttpVersion,
      "com.softwaremill.sttp.client3" %% "http4s-backend" % sttpVersion,
      "org.http4s" %% "http4s-ember-client" % http4sVersion,
      "dev.kovstas" %% "fs2-throttler" % "1.0.12",
      "com.github.cb372" %% "cats-retry" % "3.1.3",
    ),
    libraryDependencies += "org.scalameta" %% "munit" % "1.0.0" % Test,
  )
