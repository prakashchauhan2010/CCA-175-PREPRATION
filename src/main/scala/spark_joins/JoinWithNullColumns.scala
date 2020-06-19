package spark_joins
import common.ConnectionUtil

object JoinWithNullColumns extends App {
  val sc = ConnectionUtil.sc
  val spark = ConnectionUtil.spark
  import spark.implicits._

  val df1 = sc.parallelize(Seq(
    (1, Some(101), 2500),
    (2, Some(102), 1110),
    (3, None, 500))).toDF("paymentId", "customerId", "amount")

  val df2 = sc.parallelize(Seq(
    (Some(101), "Jon"),
    (Some(102), "Aron"),
    (None, "Sam"))).toDF("customerId", "name")

  // This will give atleast one duplicate column based on the join condition.
  // Also JOIN has ignored the null columns.
  df1.join(df2, df1.col("customerId") === df2.col("customerId")).show()

  /* Output
   *+---------+----------+------+----------+----+
    |paymentId|customerId|amount|customerId|name|
    +---------+----------+------+----------+----+
    |        1|       101|  2500|       101| Jon|
    |        2|       102|  1110|       102|Aron|
    +---------+----------+------+----------+----+
  */

  // Providing JOIN columns in a Sequence will prevent duplicate columns when joining two dataframes.
  // Also JOIN has ignored the null columns.
  df1.join(df2, Seq("customerId")).show()

  /* Output
  +----------+---------+------+----+
  |customerId|paymentId|amount|name|
  +----------+---------+------+----+
  |       101|        1|  2500| Jon|
  |       102|        2|  1110|Aron|
  +----------+---------+------+----+

*/
  // One way of selecting specific columns from a specific dataframe.
  // Give alias to all the dataframe used in JOIN condition.
  // Also JOIN has ignored the null columns.
  df1.as("df1").join(df2.as("df2"), df1.col("customerId") === df2.col("customerId"))
    .select("df1.paymentId", "df1.customerId", "df1.amount").show()

  /*Output
   *
    +---------+----------+------+
    |paymentId|customerId|amount|
    +---------+----------+------+
    |        1|       101|  2500|
    |        2|       102|  1110|
    +---------+----------+------+
  */

  // The <=> operator is an Equality test operator that is safe to use when the columns have null values.
  // It treats null as a value.
  // Use alias to ignore the duplicate columns
  df1.as("df1").join(df2.as("df2"), df1.col("customerId") <=> df2.col("customerId"))
    .select("df1.paymentId", "df1.customerId", "df1.amount").show()

  /*
    +---------+----------+------+
    |paymentId|customerId|amount|
    +---------+----------+------+
    |        1|       101|  2500|
    |        2|       102|  1110|
    |        3|      null|   500|
    +---------+----------+------+
*/
}