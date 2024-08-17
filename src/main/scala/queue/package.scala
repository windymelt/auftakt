package dev.capslock.auftakt

import doobie.*
import doobie.implicits.{*, given}
import doobie.postgres.implicits.*

import java.time.OffsetDateTime

package object queue {
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
}
