package dev.capslock.auftakt

import cats.effect.IO
import cats.effect.Resource
import cats.syntax.all.{*, given}
import doobie.*
import doobie.implicits.{*, given}
import doobie.postgres.implicits.*
import doobie.util.transactor.Transactor

// Active instance occupies DB by acquiring lock.
// Stand-by instance waits for lock with same key.
// When active instance is dead, stand-by instance acquires lock and take over.
val dbLockResource: String => Transactor[IO] => Resource[IO, String] =
  lockKey =>
    xa => {
      val logger = scribe.cats[IO]
      Resource
        .make[IO, String] {
          val logLocking =
            logger.info(s"Acquiring postgres advisory lock [${lockKey}] ...")
          val logLocked = logger.warn("Lock acquired. Switched to active node!")
          val locking: IO[Unit] =
            sql"SELECT pg_advisory_lock(hashtext(${lockKey}));"
              .query[Unit]
              .unique
              .transact(xa)

          logLocking >> locking >> logLocked >> lockKey.pure[IO]
        } { k =>
          // Even if application clashed, Postgres automatically discard all lock in this session.
          val logUnlocking = logger.warn(
            "Releasing postgres advisory lock. Switching to standby node!",
          )
          val unlocking = sql"SELECT pg_advisory_unlock(hashtext(${k}));"
            .query[Unit]
            .unique
            .transact(xa)

          logUnlocking >> unlocking
        }
    }
