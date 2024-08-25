package dev.capslock.auftakt

// Thanks to @tazato (https://discord.com/channels/632277896739946517/632727524434247691/1276221501086892098)

import scala.concurrent.duration.*

import cats.effect.kernel.Async
import doobie.ConnectionIO
import doobie.WeakAsync
import doobie.syntax.all.*
import doobie.util.transactor.Transactor
import cats.effect.IO

/** USE THIS WITH CAUTION: Think of this as a mutex around a specific effect.
  *
  * If the downstream effect involves any `.transact(tx)` and you're processing
  * unbounded in parallel you WILL lock up the entire application because all
  * your connections in the hikari pool are deadlocked on the advisory lock.
  *
  * This doesn't prevent processing of the effect `f` more than once, so the
  * effect itself needs to check at the very top if it should halt or not.
  */
trait LockedEffect[F[_]] {

  /** Aquire a lock by name, run effect, release lock. RAII pattern (kinda)
    *
    * @param lockKey
    *   If multiple events try to aquire a lock with the same name, they will
    *   run mutually exclusively. If multiple events try to aquire different
    *   named locks, they will not be synchronized at all.
    *
    * @param effectToRun
    *   The effect to run inside the locked context.
    * @return
    */
  def lock(lockKey: String)(
      effectToRun: F[Unit],
  ): F[Unit]
}

object LockedEffect {

  /** Uses postgres as the backing distributed mutex.
    *
    * @param F
    * @param tx
    * @return
    */
  def distributedImpl[F[_]](using
      F: Async[F],
      tx: Transactor[F],
  ) = new LockedEffect[F] {

    /** Aquire a lock by name. DO NOT USE THIS WITH UNBOUND PARALLELISM! If the
      * entire available connection pool is exhausted and the downstream
      * `effectToRun` has any database interactions contained within (very
      * likely) the application will deadlock
      *
      * @param lockKey
      * @param effectToRun
      * @return
      */
    override def lock(lockKey: String)(effectToRun: F[Unit]): F[Unit] = {
      WeakAsync.liftK[F, ConnectionIO].use { lifter =>

        val connectionIOF = for {
          _ <- lifter(
            scribe
              .cats[F]
              .info(s"Acquiring postgres advisory lock [${lockKey}] ..."),
          )
          _ <- sql"""SELECT pg_advisory_xact_lock(hashtext(${lockKey}));"""
            .query[Unit]
            .unique
          _ <- lifter(
            scribe.cats[F].info("Postgres advisory lock acquired."),
          )
          _ <- lifter(effectToRun)
          _ <- lifter(
            scribe.cats[F].info("Releasing advisory lock..."),
          )
        } yield ()

        connectionIOF
          .transact(tx)
      }
    }
  }
}
