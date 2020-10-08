package meteor

import cats.effect.{ContextShift, IO, Timer}
import cats.implicits._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks

import scala.concurrent.ExecutionContext.global

class PutOpSpec
    extends AnyFlatSpec
    with Matchers
    with ScalaCheckDrivenPropertyChecks {
  implicit val timer: Timer[IO] = IO.timer(global)
  implicit val cs: ContextShift[IO] = IO.contextShift(global)

  behavior.of("put operation")

  it should "success inserting item with both keys" in forAll {
    test: TestData =>
      val tableName = Table("test_primary_keys")
      val result = Client.resource[IO].use { client =>
        client.put[TestData](test, tableName)
      }
      result.unsafeRunSync() shouldBe an[Unit]
  }

  it should "return old value after successfully inserting item with both keys" in forAll {
    old: TestData =>
      val tableName = Table("test_primary_keys")
      val updated = old.copy(data = old.data + "-updated")
      val result =
        Client.resource[IO].use { client =>
          client.put[TestData](old, tableName) >> client.put[
            TestData,
            TestData
          ](updated, tableName)
        }
      result.unsafeRunSync() shouldEqual Some(old)
  }

  it should "success inserting item without sort key" in forAll {
    test: TestDataSimple =>
      val tableName = Table("test_partition_key_only")
      val result = Client.resource[IO].use { client =>
        client.put[TestDataSimple](test, tableName)
      }
      result.unsafeRunSync() shouldBe an[Unit]
  }

  it should "return old value after successfully inserting item without sort key" in forAll {
    old: TestDataSimple =>
      val tableName = Table("test_partition_key_only")
      val updated = old.copy(data = old.data + "-updated")
      val result = Client.resource[IO].use { client =>
        client.put[TestDataSimple](old, tableName) >>
          client.put[TestDataSimple, TestDataSimple](
            updated,
            tableName
          )
      }
      result.unsafeRunSync() shouldEqual Some(old)
  }
}
