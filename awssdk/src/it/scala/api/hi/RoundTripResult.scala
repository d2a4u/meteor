package meteor
package api.hi

case class RoundTripResult[T](
  wrote: T,
  read: Option[TestData]
)
