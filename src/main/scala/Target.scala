package dev.capslock.auftakt

import cats.effect.IO
import cats.effect.std.Queue
import dev.capslock.auftakt.queue.QueueRow
import fs2.Pipe
import fs2.Stream
import sttp.client3.Response
import sttp.model.Uri

import scala.concurrent.duration.FiniteDuration

enum ThrottlingStrategy:
  case FixedRate(limit: Int, period: FiniteDuration)
  case TokenBucket(limit: Int, period: FiniteDuration)
  case TokenBucketDrop(limit: Int, period: FiniteDuration)

enum RetryStrategy:
  case NoRetry
  case FixedDelay(delay: FiniteDuration, maxRetries: Int)
  case ExponentialBackoff(initialDelay: Int, maxRetries: Int)

case class TargetConfig(
    name: String,
    concurrencyLimit: Option[Int],
    bufferingQueueLength: Option[Int] = Some(10),
    deadLetterQueueLength: Option[Int] = Some(100),
    throttlingStrategy: ThrottlingStrategy,
    retryStrategy: RetryStrategy,
)

case class Target(id: Long, config: TargetConfig) {
  type ThrottlingToken = Unit
  type Resp            = Response[Either[String, String]]

  def intake(body: QueueRow => IO[Unit]): Pipe[IO, QueueRow, Unit] = stream => {
    for { // TODO: DLQ
      q <- Stream.eval(Queue.synchronous[IO, ThrottlingToken])
      throttling <- Stream.eval(
        throttlingStream(config.throttlingStrategy)
          .evalMap(_ => q.offer(()))
          .compile
          .drain
          .start, // TODO: kill throttling stream when done
      )
      concurrentStream <- stream.through(
        concurrent(config.concurrencyLimit, q, body),
      )
    } yield concurrentStream
  }

  private def throttlingStream(
      strategy: ThrottlingStrategy,
  ): Stream[IO, ThrottlingToken] = {
    import dev.kovstas.fs2throttler.Throttler._

    strategy match
      case ThrottlingStrategy.FixedRate(limit, period) =>
        Stream.constant(()).covary[IO].metered(period / limit)
      case ThrottlingStrategy.TokenBucket(limit, period) =>
        Stream.constant(()).covary[IO].through(throttle(limit, period, Shaping))
      case ThrottlingStrategy.TokenBucketDrop(limit, period) =>
        Stream
          .constant(())
          .covary[IO]
          .through(throttle(limit, period, Enforcing))
  }

  private def concurrent(
      concurrencyLimit: Option[Int],
      throttlingTokenQueue: Queue[IO, ThrottlingToken],
      body: QueueRow => IO[Unit],
  ): Pipe[IO, QueueRow, Unit] =
    stream => {
      concurrencyLimit match {
        case Some(limit) =>
          stream.parEvalMapUnordered(limit)(
            withRetryToken(throttlingTokenQueue, body),
          )
        case None =>
          stream.parEvalMapUnorderedUnbounded(
            withRetryToken(throttlingTokenQueue, body),
          )
      }
    }

  private def withRetryToken(
      runTokenQueue: Queue[IO, Unit],
      body: QueueRow => IO[Unit],
  ): QueueRow => IO[Unit] = row => runTokenQueue.take >> body(row)
}
