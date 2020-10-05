package meteor

import java.time.Instant

import org.scalacheck.{Arbitrary, Gen}

object Arbitraries {
  implicit val genInstant: Gen[Instant] = Gen.calendar.map(_.toInstant)
  implicit val arbInstant: Arbitrary[Instant] = Arbitrary(genInstant)
}
