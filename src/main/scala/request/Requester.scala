package dev.capslock.auftakt.request

import cats.effect.IO
import sttp.client3._
import sttp.client3.http4s.Http4sBackend
import sttp.model.Uri

val clientResource = Http4sBackend.usingDefaultEmberClientBuilder[IO]()

def request(uri: Uri, body: Array[Byte]): IO[Response[Either[String, String]]] =
  clientResource.use { c =>
    val req = basicRequest.body(body).post(uri)
    c.send(req)
  }
