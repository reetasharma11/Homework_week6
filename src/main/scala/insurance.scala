import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object Exercise2 {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

   def main(args:Array[String]){
      val spark =
        SparkSession
          .builder()
          .master("local[2]")
          .appName("Dataset-Basic")
          .getOrCreate()
      println("Read insurance.csv file (uploaded in slack channel week)")
      val ds = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("/home/reeta/IdeaProjects/Homework6/src/main/scala/insurance.csv")

      println("Print the size")
      println(ds.count()+"\n")

      println("Print sex and count of sex (use group by in sql)")
      ds.groupBy(col1 = "sex").count().show
      println("\n")

      println("Filter smoker=yes and print again the sex,count of sex")
      ds.select("sex","smoker").where("smoker == 'yes'" ).groupBy(col1 = "sex").count().show
      println("\n")

      println("Group by region and sum the charges (in each region), then print rows by\ndescending order (with respect to sum)")
      ds.groupBy(col1 = "region").sum("charges").orderBy(desc("sum(charges)")).show
      println("\n")

      spark.stop()


    }
}


