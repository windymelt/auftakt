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
import sttp.client3.Response

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

    // POSTs specified url.
    // TODO: Retry. blocking operation. Hard retry(requeueing).
    // TODO: Throttle strategy each target.
    val dispatch: QueueRow => IO[Response[Either[String, String]]] =
      (r: QueueRow) =>
        for {
          _ <- scribe.cats[IO].info(s"dispatching ${r.id}")
          resp <- request.request(
            sttp.model.Uri.unsafeParse(
              r.targetUrl,
            ), // TODO: mark as failed if parse failed
            r.payload,
          )
          // TODO: save error message if failed (into some table)
        } yield resp

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

    val defaultTarget = Target(
      0L,
      TargetConfig(
        name = "default",
        concurrencyLimit = Some(4),
        bufferingQueueLength = Some(10),
        deadLetterQueueLength = Some(100),
        throttlingStrategy =
          ThrottlingStrategy.TokenBucket(3, FiniteDuration(1, "second")),
        retryStrategy = RetryStrategy.NoRetry,
      ),
    )

    def executeQueue(row: QueueRow)(using xa: Transactor[IO]) = for {
      resp <- dispatch(row)
      _ <- IO
        .pure(resp.isSuccess)
        .ifM(
          mark(QueueStatus.finished)(row),
          scribe.cats[IO].warn("Job finished not successfully") >> mark(
            QueueStatus.finished, // TODO: mark as error
          )(row),
        )
    } yield ()

    def polling(using xa: Transactor[IO]) = for {
      _ <- scribe.cats[IO].info("loading queue")
      _ <- markVacantQueueAsGrabbed(grabberId)
      _ <- grabbedRows(grabberId)
        .evalTap { row =>
          scribe.cats[IO].debug(row.toString)
        } // we don't need to mark row as processing: until all grabbed rows are processed, no further process are executed
        .through(
          defaultTarget.intake(executeQueue),
        ) // TODO: dispatch to dynamically specified target
        .compile
        .drain
    } yield ()

    for {
      // TODO: attempt to retry to connect to DB when connection failed
      // TODO: halt when any of subsystem is down
      // TODO: configurable instance key
      // TODO: resurrect (reset grabber and mark as waiting) stale (grabbed but not processed for long time) queue
      // TODO: mark looped queue as error
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
