package com.arquivei.pubsubgenericeventparser

import com.spotify.scio.coders.Coder
import org.json4s._

object Coders {
  implicit val coder: Coder[JValue] = Coder.kryo[JValue]
}
