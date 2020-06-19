package spark_joins
import common.ConnectionUtil

object SelfJoinExample extends App {
  val sc = ConnectionUtil.sc
  val spark = ConnectionUtil.spark
  import spark.implicits._

  /*
 *
  +----------+------------+---------+
  |employeeId|employeeName|managerId|
  +----------+------------+---------+
  |         1|         ceo|     null|
  |         2|    manager1|        1|
  |         3|    manager2|        1|
  |       101|         Amy|        2|
  |       102|         Sam|        2|
  |       103|        Aron|        3|
  |       104|       Bobby|        3|
  |       105|         Jon|        3|
  +----------+------------+---------+
  */

  val employee1 = spark.createDataFrame(Seq(
    (1, "ceo", None),
    (2, "manager1", Some(1)),
    (3, "manager2", Some(1)),
    (101, "Amy", Some(2)),
    (102, "Sam", Some(2)),
    (103, "Aron", Some(3)),
    (104, "Bobby", Some(3)),
    (105, "Jon", Some(3)))).toDF("employeeId", "employeeName", "managerId")

  val self_join = employee1.as("t1").join(employee1.as("t2"), $"t1.managerId" === $"t2.employeeId")
    .select($"t1.employeeName".as("Employee Name"), $"t2.employeeName".as("Manager Name"))
  self_join.show()
  self_join.printSchema()
}