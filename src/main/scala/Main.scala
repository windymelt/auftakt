import doobie._
import doobie.implicits._
import cats._
import cats.effect._
import cats.implicits._
import doobie.util.ExecutionContexts

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

    val program = for {
      n <- sql"select 42".query[Int].unique
    } yield n

    for {
      n <- program.transact(xa)
      _ <- IO.println(n)
    } yield ()
  }
}
