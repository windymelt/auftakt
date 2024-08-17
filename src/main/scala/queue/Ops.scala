package dev.capslock.auftakt.queue

import cats.data.NonEmptyList
import cats.effect.IO
import doobie.*
import doobie.implicits.{*, given}
import doobie.postgres.implicits.*

object Ops {
  def grabbedRows(
      grabberId: GrabberId,
  )(using xa: Transactor[IO]): fs2.Stream[IO, QueueRow] =
    sql"SELECT id, dag_id, node_id, grabber_id, prerequisite_node_ids, created_at, target_url, payload, status, run_after FROM queue WHERE grabber_id = ${grabberId} AND status = ${QueueStatus.grabbed} LIMIT 100"
      .query[QueueRow]
      .stream
      .transact(xa)

  def waitingRows(using xa: Transactor[IO]): fs2.Stream[IO, QueueRow] =
    sql"SELECT id, dag_id, node_id, grabber_id, prerequisite_node_ids, created_at, target_url, payload, status, run_after FROM queue WHERE status = ${QueueStatus.waiting} LIMIT 1000"
      .query[QueueRow]
      .stream
      .transact(xa)

  // This query is atomic; so other grabbers won't grab the same row.
  def markVacantQueueAsGrabbed(
      grabberId: GrabberId,
  )(using xa: Transactor[IO]): IO[Int] =
    sql"UPDATE queue SET status = ${QueueStatus.grabbed}, grabber_id = ${grabberId} WHERE status = ${QueueStatus.claimed}".update.run
      .transact(xa)

  def mark(using xa: Transactor[IO]): QueueStatus => QueueRow => IO[Int] =
    s =>
      r =>
        sql"UPDATE queue SET status = ${s} WHERE id = ${r.id}".update.run
          .transact(xa)

  def removeFromQueue(using xa: Transactor[IO]): QueueRow => IO[Int] =
    (r: QueueRow) =>
      sql"DELETE FROM queue WHERE id = ${r.id}".update.run.transact(xa)

  def isAllFinished(using
      xa: Transactor[IO],
  ): Option[DagId] => Set[NodeId] => IO[Boolean] = dagId =>
    ns =>
      scribe.cats[IO].debug("checking finish") >> (NonEmptyList.fromList(
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
            case _                    => false // Lacking of prerequisite nodes
          }
      })
}
