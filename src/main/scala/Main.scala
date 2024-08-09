import doobie.*
import doobie.implicits.{*, given}
import cats.*
import cats.data.Kleisli
import cats.effect.{*, given}
import cats.implicits.{*, given}
import doobie.util.ExecutionContexts
import doobie.postgres.implicits.*

import java.time.OffsetDateTime
import scala.concurrent.duration.FiniteDuration

enum QueueStatus:
  case claimed, grabbed

given Meta[QueueStatus] =
  pgEnumString("queue_status", QueueStatus.valueOf, _.toString)

case class QueueRow(
    id: Long,
    createdAt: OffsetDateTime,
    targetUrl: String,
    payload: Array[Byte],
    status: QueueStatus,
)

object Main extends IOApp.Simple {
  def run: IO[Unit] = {
    val xa = Transactor.fromDriverManager[IO](
      driver = "org.postgresql.Driver", // driver classname
      url =
        "jdbc:postgresql://localhost:5432/mydb", // connect URL (driver-specific)
      user = "myuser",         // user
      password = "mypassword", // password
      logHandler = None,
    )
    // TODO: mark grabber_id
    // This query is atomic; so other grabbers won't grab the same row.
    val markVacantQueueAsGrabbed: IO[Int] =
      sql"UPDATE queue SET status = ${QueueStatus.grabbed} WHERE status = ${QueueStatus.claimed}".update.run
        .transact(xa)

    // Load grabbed rows
    val loadQueue =
      sql"SELECT id, created_at, target_url, payload, status FROM queue WHERE status = ${QueueStatus.grabbed} LIMIT 100"
        .query[QueueRow]
        .stream
        .transact(xa)

    val markGrabbedQueue: QueueRow => IO[Int] = (r: QueueRow) =>
      sql"UPDATE queue SET status = ${QueueStatus.grabbed} WHERE id = ${r.id}".update.run
        .transact(xa)

    val dispatch: QueueRow => IO[QueueRow] = (r: QueueRow) =>
      IO.println(s"dispatching ${r.id}") >> IO.pure(r)

    val removeFromQueue: QueueRow => IO[Int] = (r: QueueRow) =>
      sql"DELETE FROM queue WHERE id = ${r.id}".update.run.transact(xa)

    val polling = for {
      _ <- IO.println("loading queue")
      _ <- markVacantQueueAsGrabbed
      _ <- loadQueue
        .evalTap { row =>
          IO.println(row)
        }
        .parEvalMapUnordered(4)(
          (Kleisli(dispatch) >>> Kleisli(removeFromQueue)).run,
        )
        .compile
        .drain
    } yield ()

    for {
      // poll every 1 second
      _ <- polling.andWait(FiniteDuration(1, "second")).foreverM
    } yield ()
  }
}
