package dev.capslock.auftakt

import cats.*
import cats.data.Kleisli
import cats.data.NonEmptyList
import cats.effect.{*, given}
import cats.implicits.{*, given}
import com.zaxxer.hikari.HikariConfig
import doobie.*
import doobie.hikari._
import doobie.implicits.{*, given}
import doobie.postgres.implicits.*
import doobie.util.ExecutionContexts
import doobie.util.transactor.Transactor

import java.time.OffsetDateTime
import scala.concurrent.duration.FiniteDuration

import queue.{*, given}
import queue.Ops.*

object Main extends IOApp.Simple {
  val grabberId = GrabberId(42)

  def run: IO[Unit] = {
    val xaPool: Resource[IO, HikariTransactor[IO]] = for {
      hikariConfig <- Resource.pure {
        val conf = new HikariConfig()
        conf.setDriverClassName("org.postgresql.Driver")
        conf.setJdbcUrl("jdbc:postgresql://localhost:5432/mydb")
        conf.setUsername("myuser")
        conf.setPassword("mypassword")
        conf
      }
      xa <- HikariTransactor.fromHikariConfig(
        hikariConfig,
        Some(ScribeLogHandler()),
      )
    } yield xa

    // Load grabbed rows
    // TODO: Do HTTP POST. Retry. blocking operation. Hard retry(requeueing).
    // TODO: Throttle strategy each target.
    val dispatch: QueueRow => IO[QueueRow] = (r: QueueRow) =>
      scribe.cats[IO].info(s"dispatching ${r.id}") >> IO.pure(r)

    val satisfiesRunAfter: QueueRow => IO[Boolean] = r =>
      IO(OffsetDateTime.now()).map(_.compareTo(r.runAfter) > 0)

    def markAvailableQueueAsClaimed(using xa: Transactor[IO]): IO[Unit] =
      import scribe.*
      // fetch some row, verify all prerequisite nodes are finished, mark as claimed
      val logger = scribe.cats[IO]
      for {
        _ <- logger.info("finding available node...")
        nClaimed <- waitingRows
          .evalTap(r => scribe.cats[IO].debug(r.toString))
          .evalFilter(satisfiesRunAfter)
          .evalFilterAsync[IO](4)(r => // TODO: configurable check concurrency
            isAllFinished(r.dagId)(r.prerequisiteNodeIds.toSet),
          )
          .evalMap(mark(QueueStatus.claimed))
          .compile
          .count
        _ <- nClaimed match {
          case 0L => IO.unit
          case _ =>
            logger.info("Marked rows as claimed", data("count", nClaimed))
        }
      } yield ()

    def polling(using xa: Transactor[IO]) = for {
      _ <- scribe.cats[IO].info("loading queue")
      _ <- markVacantQueueAsGrabbed(grabberId)
      _ <- grabbedRows(grabberId)
        .evalTap { row =>
          scribe.cats[IO].debug(row.toString)
        }
        .parEvalMapUnordered(4)(
          (Kleisli(dispatch) >>> Kleisli(mark(QueueStatus.finished))).run,
        )
        .compile
        .drain
    } yield ()

    for {
      // TODO: attempt to retry to connect to DB when connection failed
      // TODO: halt when any of subsystem is down
      // TODO: configurable instance key
      // TODO: resurrect (reset grabber and mark as waiting) stale (grabbed but not processed for long time) queue
      instanceKey <- IO.pure(
        "auftakt-instance-0",
      ) // share this key among active and stand-by
      _ <- xaPool.use { implicit xa =>
        for {
          _ <- LockedEffect.distributedImpl[IO].lock(instanceKey) {
            for {
              _ <- scribe.cats[IO].info("starting scheduler...")
              _ <- markAvailableQueueAsClaimed
                .andWait(FiniteDuration(5, "second"))
                .foreverM
                .start
              // poll every 1 second
              _ <- polling.andWait(FiniteDuration(1, "second")).foreverM
            } yield ()
          }
        } yield ()
      }
    } yield ()
  }
}
