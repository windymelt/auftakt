import cats.*
import cats.data.Kleisli
import cats.data.NonEmptyList
import cats.effect.{*, given}
import cats.implicits.{*, given}
import doobie.*
import doobie.implicits.{*, given}
import doobie.postgres.implicits.*
import doobie.util.ExecutionContexts
import doobie.util.transactor.Transactor

import java.time.OffsetDateTime
import scala.concurrent.duration.FiniteDuration

opaque type DagId = Int
object DagId {
  def apply(id: Int): DagId = id

  given Meta[DagId] = Meta.IntMeta
}

opaque type NodeId = Long
object NodeId {
  def apply(id: Long): NodeId = id

  given Meta[NodeId]                                        = Meta.LongMeta
  given (using mai: Meta[Array[Long]]): Meta[Array[NodeId]] = mai
}

opaque type GrabberId = Int
object GrabberId {
  def apply(id: Int): GrabberId = id

  given Meta[GrabberId] = Meta.IntMeta
}

enum QueueStatus:
  case waiting, claimed, grabbed, finished

given Meta[QueueStatus] =
  pgEnumString("queue_status", QueueStatus.valueOf, _.toString)

case class QueueRow(
    id: Long,
    dagId: Option[DagId],
    nodeId: Option[NodeId],
    grabberId: Option[GrabberId],
    prerequisiteNodeIds: Array[NodeId],
    createdAt: OffsetDateTime,
    targetUrl: String,
    payload: Array[Byte],
    status: QueueStatus,
    runAfter: OffsetDateTime,
)

object Main extends IOApp.Simple {
  val grabberId = GrabberId(42)

  def loadQueue(using xa: Transactor[IO]): fs2.Stream[IO, QueueRow] =
    sql"SELECT id, dag_id, node_id, grabber_id, prerequisite_node_ids, created_at, target_url, payload, status, run_after FROM queue WHERE status = ${QueueStatus.grabbed} LIMIT 100"
      .query[QueueRow]
      .stream
      .transact(xa)

  def waitingRows(using xa: Transactor[IO]): fs2.Stream[IO, QueueRow] =
    sql"SELECT id, dag_id, node_id, grabber_id, prerequisite_node_ids, created_at, target_url, payload, status, run_after FROM queue WHERE status = ${QueueStatus.waiting} LIMIT 1000"
      .query[QueueRow]
      .stream
      .transact(xa)

  def run: IO[Unit] = {
    given xa: Transactor[IO] = Transactor.fromDriverManager[IO](
      driver = "org.postgresql.Driver", // driver classname
      url =
        "jdbc:postgresql://localhost:5432/mydb", // connect URL (driver-specific)
      user = "myuser",         // user
      password = "mypassword", // password
      logHandler = None,       // Some(LogHandler.jdkLogHandler[IO]),
    )

    // This query is atomic; so other grabbers won't grab the same row.
    val markVacantQueueAsGrabbed: IO[Int] =
      sql"UPDATE queue SET status = ${QueueStatus.grabbed}, grabber_id = ${grabberId} WHERE status = ${QueueStatus.claimed}".update.run
        .transact(xa)

    // Load grabbed rows
    // TODO: Do HTTP POST. Retry. blocking operation. Hard retry(requeueing).
    // TODO: Throttle strategy each target.
    val dispatch: QueueRow => IO[QueueRow] = (r: QueueRow) =>
      scribe.cats[IO].info(s"dispatching ${r.id}") >> IO.pure(r)

    val mark: QueueStatus => QueueRow => IO[Int] = s =>
      r =>
        sql"UPDATE queue SET status = ${s} WHERE id = ${r.id}".update.run
          .transact(xa)

    val removeFromQueue: QueueRow => IO[Int] = (r: QueueRow) =>
      sql"DELETE FROM queue WHERE id = ${r.id}".update.run.transact(xa)

    val satisfiesRunAfter: QueueRow => IO[Boolean] = r =>
      IO(OffsetDateTime.now()).map(_.compareTo(r.runAfter) > 0)

    val isAllFinished: Option[DagId] => Set[NodeId] => IO[Boolean] = dagId =>
      ns =>
        scribe.cats[IO].info("checking finish") >> (NonEmptyList.fromList(
          ns.toList,
        ) match {
          case None => IO.pure(true)
          case Some(nsNel) =>
            val q =
              fr"SELECT status FROM queue WHERE dag_id = ${dagId} AND " ++ Fragments
                .in(
                  fr"node_id",
                  nsNel,
                )
            val statuses = q
              .query[QueueStatus]
              .stream
              .map(_ == QueueStatus.finished)
              .transact(xa)
              .compile
              .toList
            for {
              ss <- statuses
            } yield ss.size match {
              case n if n == nsNel.size => ss.fold(true)(_ && _)
              case _ => false // Lacking of prerequisite nodes
            }
        }).debug("isAllFinished")

    val markAvailableQueueAsClaimed: IO[Unit] =
      // fetch some row, verify all prerequisite nodes are finished, mark as claimed
      scribe.cats[IO].info("finding available node...") >>
        waitingRows
          .evalTap(r => scribe.cats[IO].debug(r.toString))
          .evalFilter(satisfiesRunAfter)
          .evalFilter(r =>
            isAllFinished(r.dagId)(r.prerequisiteNodeIds.toSet),
          ) // TODO: async
          .evalMap(mark(QueueStatus.claimed))
          .compile
          .drain

    val polling = for {
      _ <- scribe.cats[IO].info("loading queue")
      _ <- markVacantQueueAsGrabbed
      _ <- loadQueue
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
      _ <- scribe.cats[IO].info("starting scheduler...")
      _ <- markAvailableQueueAsClaimed
        .andWait(FiniteDuration(5, "second"))
        .foreverM
        .start
      // poll every 1 second
      _ <- polling.andWait(FiniteDuration(1, "second")).foreverM
    } yield ()
  }
}
