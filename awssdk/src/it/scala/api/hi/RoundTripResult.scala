package meteor
package api.hi

private[meteor] case class RoundTripResult[T](
  wrote: T,
  read: Option[TestData]
)
