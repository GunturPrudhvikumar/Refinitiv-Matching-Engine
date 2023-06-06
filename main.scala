import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

case class Order(orderId: String, username: String, orderTime: Long, orderType: String, quantity: Long, price: Long)

object FxMatchingEngine {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("FxMatchingEngine")
      .master("local[*]") // Change to appropriate Spark master URL for your setup as i am using local
      .getOrCreate()

    import spark.implicits._

    //Define the schema for the CSV file
    val schema = StructType(seq(
      StructField("orderId", StringType, nullable = false),
      StructField("username", StringType, nullable = false),
      StructField("orderTime", LongType, nullable = false),
      StructField("orderType", StringType, nullable = false),
      StructField("quantity", LongType, nullable = false),
      StructField("price", LongType, nullable = false)
    ))


    // Step 1: Read the CSV file into a DataFrame
    val ordersDF = spark.read.option("header", "false")
      .schema(schema)
      .csv("exampleOrders.csv")
      .as[Order]

    // Step 2: Create the order book DataFrame with unmatched orders
    var orderBookDF = ordersDF.filter($"orderType" === "buy").withColumn("matched", lit(false))

    // Step 3: Match orders with the same quantity
    val matchedOrdersDF = ordersDF.filter($"orderType" === "sell")
      .join(orderBookDF, Seq("quantity"))
      .filter(!$"matched")
      .withColumn("matched", lit(true))

    // Step 4: Update the order book and close matched orders
    orderBookDF = orderBookDF.except(matchedOrdersDF.select(orderBookDF.columns.head, orderBookDF.columns.tail: _*))
    val closedOrdersDF = matchedOrdersDF.select($"orderId".as("order1Id"), $"username".as("order1Username"), $"orderTime".as("order1Time"),
      $"orderId".as("order2Id"), $"orderTime".as("order2Time"), $"quantity", $"price")

    // Step 5: Write the matched orders to a CSV file
    closedOrdersDF.write.mode("overwrite").csv("outputExampleMatches.csv")

    // Step 6: Print the matched orders
    closedOrdersDF.show()

    // Step 7: Print the remaining unmatched orders in the order book
    orderBookDF.show()
  }
}
