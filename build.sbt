val scala3Version = "3.4.2"

lazy val root = project
  .in(file("."))
  .settings(
    name         := "doobie exercise",
    version      := "0.1.0-SNAPSHOT",
    scalaVersion := scala3Version,
    fork         := true,
    libraryDependencies ++= Seq(
      // Start with this one
      "org.tpolecat" %% "doobie-core"     % "1.0.0-RC4",
      "org.tpolecat" %% "doobie-hikari"   % "1.0.0-RC4", // HikariCP transactor.
      "org.tpolecat" %% "doobie-postgres" % "1.0.0-RC4",
    ),
    libraryDependencies += "org.scalameta" %% "munit" % "1.0.0" % Test,
  )
