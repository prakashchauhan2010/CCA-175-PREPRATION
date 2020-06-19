package spark_joins
import common.ConnectionUtil
object Spark_Joins_Dataset extends App {
  val sc = ConnectionUtil.sc
  val spark = ConnectionUtil.spark
  import spark.implicits._

  /*Payment Data.
   *+---------+----------+------+
    |paymentId|customerId|amount|
    +---------+----------+------+
    |        1|       101|  2500|
    |        2|       102|  1110|
    |        3|       103|   500|
    |        4|       104|   400|
    |        5|       105|   150|
    |        6|       106|   450|
    +---------+----------+------+
   *
   * Customer Data.
   * +----------+----+
    |customerId|name|
    +----------+----+
    |       101| Jon|
    |       102|Aron|
    |       103| Sam|
    +----------+----+

    Available syntax for JOIN:

      df1.join(df2, $"df1Key" === $"df2Key")

      df1.join(df2, $"df1Key" === $"df2Key", "inner")

      df1.join(df2).where($"df1Key" === $"df2Key")

      df1.join(df2).filter($"df1Key" === $"df2Key")

   */

  val payment = sc.parallelize(Seq(
    (1, 101, 2500),
    (2, 102, 1110),
    (3, 103, 500),
    (4, 104, 400),
    (5, 105, 150),
    (6, 106, 450))).toDF("paymentId", "customerId", "amount")
  payment.createOrReplaceTempView("payment")

  val customer = sc.parallelize(Seq(
    (101, "Jon"),
    (102, "Aron"),
    (103, "Sam"))).toDF("customerId", "name")
  customer.createOrReplaceTempView("customer")

  case class Payment(paymentId: Int, customerId: Int, amount: Int)

  case class Customer(customerId: Int, name: String)

  // Converting dataframe into datasets.
  val paymentDS = payment.as[Payment]
  val customerDS = customer.as[Customer]

  // inner_join type: Dataset[(Customer, Payment)]
  val inner_join = customerDS.joinWith(paymentDS, customerDS.col("customerId") === paymentDS.col("customerId"))
  inner_join.show()
  inner_join.printSchema()

}