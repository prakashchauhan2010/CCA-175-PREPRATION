package spark_joins
import common.ConnectionUtil
object Spark_Joins extends App {
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

  // INNER JOIN
  //  val inner_join_df = customer.join(payment,Seq("customerId"),"inner")
  //  spark.sql("set spark.sql.crossJoin.enabled = true")
  //  val inner_join_df = customer.join(payment)
  val inner_join_df = customer.join(payment, "customerId")
  inner_join_df.show()

  // LEFT JOIN
  val left_join_df = payment.join(customer, Seq("customerId"), "left")
  left_join_df.show()

  // RIGHT JOIN
  val right_join = payment.join(customer, Seq("customerId"), "right")
  right_join.show()

  // OUTER JOIN
  val outer_join = customer.join(payment, Seq("customerId"), "outer")
  outer_join.show()

  // CROSS JOIN
  val cross_join = customer.crossJoin(payment)
  cross_join.show()

  // LEFT SEMI JOIN
  val left_semi = payment.join(customer, customer.col("customerId") === payment.col("customerId"), "left_semi")
  left_semi.show()

  /*  Returns only the data from left table that has a match on the right table based on the join condition.
   *  Return only columns from the left table.
   *  +----------+---------+------+
      |customerId|paymentId|amount|
      +----------+---------+------+
      |       101|        1|  2500|
      |       103|        3|   500|
      |       102|        2|  1110|
      +----------+---------+------+
  */

  // LEFT ANTI JOIN
  val left_anti = payment.join(customer, Seq("customerId"), "left_anti")
  left_anti.show()
  left_anti.printSchema()

}