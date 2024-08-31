package dev.capslock.auftakt

import scala.concurrent.duration.FiniteDuration

enum ThrottlingStrategy:
    case FixedRate(limit: Int, period: Int)
    case TokenBucket(limit: Int, period: Int)

enum RetryStrategy:
    case FixedDelay(delay: FiniteDuration, maxRetries: Int)
    case ExponentialBackoff(initialDelay: Int, maxRetries: Int)
