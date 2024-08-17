package dev.capslock.auftakt

import cats.effect.IO
import doobie.util.log.ExecFailure
import doobie.util.log.LogEvent
import doobie.util.log.LogHandler
import doobie.util.log.ProcessingFailure
import doobie.util.log.Success
import scribe._

class ScribeLogHandler extends LogHandler[IO] {
  val logger = scribe.cats[IO]

  def run(logEvent: LogEvent): IO[Unit] = logEvent match {
    case Success(sql, args, label, exec, processing) =>
      logger.info("Query successful", data("query", sql))
    case ProcessingFailure(sql, args, label, exec, processing, failure) =>
      logger.error(
        "Query processing failure",
        data("query", sql),
        data("processing", processing),
        data("failure", failure),
      )
    case ExecFailure(sql, args, label, exec, failure) =>
      logger.error(
        "Query exec failure",
        data("query", sql),
        data("failure", failure),
      )
  }
}
