import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object ThesisTest extends App{
  val spark = SparkSession
    .builder()
    .master("local")
    .appName("SparkSessionExample")
    .enableHiveSupport()
    .getOrCreate()

  //read schema file to generate struct type

  val schemaFile = spark.read.option("header","true").option("delimiter",",").csv("C:\\Users\\anutarar\\IdeaProjects\\thesis_test\\src\\main\\resources\\schemafile.csv")
  var structType = new StructType()
  schemaFile.foreach{
    row =>
      val dataType =row.get(1)
      val dt = dataType match {
        case "IntegerType"=> IntegerType
        case "StringType"=> StringType
        case "DateType"=> DateType
        case "DoubleType"=> DoubleType
        case _ => StringType
      }

      structType = structType.add(StructField(row.get(0).asInstanceOf[String],dt,false))
  }

  //read the data file
  val df = spark.read.option("header","false").option("delimiter",",").option("inferschema","true").csv("C:\\Users\\anutarar\\IdeaProjects\\thesis_test\\src\\main\\resources\\data-work.csv")
  import org.apache.spark.sql.functions._

  //convert date from epoch format to date time format
  val data =  df.withColumn("_c2", from_unixtime(df.col("_c2")).cast("date"))

  //filter bad rows
  val badRecords: Dataset[Row] = data.filter{ row => row.length!=structType.length || row.anyNull}
  // subtract bad records
  val schemaMatchedRows= spark.createDataFrame(data.except(badRecords).rdd,structType)

  //combine the rows which do not match the schema and contains null and write them in parquet format
  badRecords.repartition(1).write.option("headers",structType.fieldNames.mkString(",")).parquet("C:\\Users\\anutarar\\Documents\\bad_records")

  schemaMatchedRows.repartition(1).write.option("headers",structType.fieldNames.mkString(",")).parquet("C:\\Users\\anutarar\\Documents\\success_records")

  spark.read.parquet("C:\\Users\\anutarar\\Documents\\success_records").show()
  spark.read.parquet("C:\\Users\\anutarar\\Documents\\bad_records").show()

}
