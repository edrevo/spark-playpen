import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.mllib.random.RandomRDDs._

object Main extends App {
  val spark = SparkSession
      .builder()
      .appName("test")
      .master("local[2]")
      .getOrCreate()
  import spark.implicits._

  val bootstrapServers = "localhost:9092"
  val topic = "test2"
  def processDataSet(ds: Dataset[Double]) = {
    println(ds.reduce(Math.max(_, _)))
  }

  def filter(ds: Dataset[Double]) = ds
    .write
    .format("sock")
    .option("kafka.bootstrap.servers", bootstrapServers)
    .option("topic", topic)
    .save()
  lazy val data = spark.sqlContext.read.csv("hdfs://localhost:8020/input4.csv").as[String].map(_.toDouble)
  lazy val filteredData = spark.sqlContext.read
    .format("sock")
    .option("kafka.bootstrap.servers", bootstrapServers)
    .option("subscribe", topic)
    .load()
    .as[Double]
  try {
    args.head match {
      case "filter" => filter(data)
      case "process" =>
        processDataSet(filteredData)
      case "processDirect" => processDataSet(data)
      case "generate" =>
        normalRDD(spark.sparkContext, 400000000L, 10).saveAsTextFile("hdfs://localhost:8020/input4.csv")
    }
  } finally {
    spark.stop()
  }
}
