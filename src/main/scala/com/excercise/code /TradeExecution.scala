/*****************************************************************************************************************************************************************
 * ProgramName:TradeExecution                                                                                                                                    *
 * Description:                                                                                                                                                  *
 *             The TradeExecution code is responsible for processing trade orders and matching them between an order book and an order file.                     *
 *              It reads data from CSV files, performs matching operations, and produces output files.                                                           *
 *              The code is implemented using Spark and Scala.                                                                                                   *
 *                                                                                                         																											 *
 ****************************************************************************************************************************************************************/

package com.excercise.code
import java.util.Calendar
import java.text.SimpleDateFormat
import org.apache.spark.sql._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{FileSystem, Path,FileUtil}
import org.apache.spark.sql.SaveMode
	
object TradeExecution {
 val spark=GetSparkSession.getSparkSession("TradeMatchEngine")
 spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") //disables the creation of _SUCCESS files
 spark.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") 
  import spark.implicits._  
  val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
  val format = new SimpleDateFormat("yyyyMMddHHmmss");
  val calenderInstance = Calendar.getInstance();
  val ts=format.format(calenderInstance.getTime)
  fs.setVerifyChecksum(false)
 
//The below method reads a csv file with the provided schema and return the result in a dataframe.
  def readCsvFile(filePath:String,Schema:StructType):DataFrame={
    val fp=new Path(s"$filePath")
    if(fs.exists(fp))
    {
      spark.read.format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat").option("header", "false").schema(Schema).load(filePath)
      
    }
    
    else {
      spark.createDataFrame(spark.sparkContext.emptyRDD[Row], Schema)
    }
    
   
  }
 
 //The below method moves the file from sourcepath to destination path
  def archiveFile(sourcePath:String,destinationPath:String)={ 
    if(fs.exists(new Path(sourcePath)))
    {
    fs.rename(new Path(sourcePath), new Path(destinationPath))
    }
    else println(s"$sourcePath does not exists")
    
  }


  /*matchOrdersBetweenOrderBookAndOrderFile method tries to match the orders between orderbook and orderfile.
   * Then returns the matchedorders from orderBook , unmatched orders from orderBook and orderFiles
  */
  def matchOrdersBetweenOrderBookAndOrderFile(orderBookDF:DataFrame,orderFileDF:DataFrame):(DataFrame,DataFrame,DataFrame)={
    val orderBookBuyOrdersRankedDF=orderBookDF.where("order_type='BUY'").withColumn("rno",row_number() over(Window.partitionBy("quantity").orderBy("order_time")))
    val orderBookSellOrdersRankedDF=orderBookDF.where("order_type='SELL'").withColumn("rno",row_number() over(Window.partitionBy("quantity").orderBy("order_time")))
    val orderFileBuyOrderRankedDF=orderFileDF.where("order_type='BUY'").withColumn("rno",row_number() over(Window.partitionBy("quantity").orderBy(col("price").desc,col("order_time").desc)))
    val orderFileSellOrderRankedDF=orderFileDF.where("order_type='SELL'").withColumn("rno",row_number() over(Window.partitionBy("quantity").orderBy(col("price"),col("order_time").desc)))
    val orderBookBuyOrdersMatchedDF=orderBookBuyOrdersRankedDF.as("books").join(orderFileSellOrderRankedDF.as("file"),col("books.quantity")===col("file.quantity")&&col("books.rno")===col("file.rno"))
                               .withColumn("match_order_id",when(col("books.order_time")>col("file.order_time"),col("books.order_id")).otherwise(col("file.order_id")))
                                      .withColumn("first_order_id",when(col("books.order_time")>col("file.order_time"),col("file.order_id")).otherwise(col("books.order_id")))
                                       .withColumn("order_time_matched",greatest(col("file.order_time"),col("books.order_time")))
                                       .drop("file.order_time","books.order_time")
                                      .withColumn("quantity_matched",col("books.quantity"))
                                      .withColumn("order_price",col("books.price"))
                                      .select("match_order_id","first_order_id","order_time_matched","quantity_matched","order_price")
   val orderBookSellOrdersMatchedDF=orderBookSellOrdersRankedDF.as("books").join(orderFileBuyOrderRankedDF.as("file"),col("books.quantity")===col("file.quantity")&&col("books.rno")===col("file.rno"))
                               .withColumn("match_order_id",when(col("books.order_time")>col("file.order_time"),col("books.order_id")).otherwise(col("file.order_id")))
                                      .withColumn("first_order_id",when(col("books.order_time")>col("file.order_time"),col("file.order_id")).otherwise(col("books.order_id")))
                                      .withColumn("order_time_matched",greatest(col("file.order_time"),col("books.order_time")))
                                      .withColumn("quantity_matched",col("books.quantity"))
                                      .withColumn("order_price",col("books.price"))
                                      .drop("file.order_time","books.order_time")
                                      .select("match_order_id","first_order_id","order_time_matched","quantity_matched","order_price")
   val orderBookFileOrdersMatchedDF=orderBookBuyOrdersMatchedDF.union(orderBookSellOrdersMatchedDF)
   val closedOrderIdsDF=orderBookFileOrdersMatchedDF.select(array("match_order_id","first_order_id").as("closedOrdersArray"))
                                   .withColumn("order_id",explode(col("closedOrdersArray")))
                                   .select("order_id")
   val unClosedOrdersFromBookDF=orderBookDF.join(closedOrderIdsDF,Seq("order_id"),"left_anti")
   val unClosedOrdersFromFileDF=orderFileDF.join(closedOrderIdsDF,Seq("order_id"),"left_anti")
   (orderBookFileOrdersMatchedDF,unClosedOrdersFromBookDF,unClosedOrdersFromFileDF)
  
  }
  
  /* matchOrderWithInTheOrderFile method tries to match the orders with in the orderFile that are left over from matching between orderBook and OrderFile.
   * Then returns the matched and unmatched orders from orderFile
   */
  
  def matchOrderWithInTheOrderFile(orderFileDF:DataFrame):(DataFrame,DataFrame)={
    val buyOrdersRankedDF=orderFileDF.where("order_type='BUY'").withColumn("rno",row_number() over(Window.partitionBy("quantity").orderBy("order_time")))
    val sellOrdersRankedDF=orderFileDF.where("order_type='SELL'").withColumn("rno",row_number() over(Window.partitionBy("quantity").orderBy("order_time")))     
    val closedOrdersDF=buyOrdersRankedDF.as("buyorders")
                                      .join(sellOrdersRankedDF.as("sellorders"),col("buyorders.quantity")===col("sellorders.quantity") && col("buyorders.rno")===col("sellorders.rno"))
                                      .withColumn("match_order_id",when(col("buyorders.order_time")>col("sellorders.order_time"),col("buyorders.order_id")).otherwise(col("sellorders.order_id")))
                                      .withColumn("first_order_id",when(col("buyorders.order_time")>col("sellorders.order_time"),col("sellorders.order_id")).otherwise(col("buyorders.order_id")))
                                      .withColumn("order_time_matched",greatest(col("sellorders.order_time"),col("buyorders.order_time")))
                                      .drop("sellorders.order_time","buyorders.order_time")
                                      .withColumn("quantity_matched",col("sellorders.quantity"))
                                      .withColumn("order_price",when(col("buyorders.order_time")>col("sellOrders.order_time"),col("sellorders.price")).otherwise(col("buyorders.price")))
                                      .select("match_order_id","first_order_id","order_time_matched","quantity_matched","order_price")
  
   val closedOrderIdsDF=closedOrdersDF.select(array("match_order_id","first_order_id").as("closedOrdersArray"))
                                    .withColumn("order_id",explode(col("closedOrdersArray")))
                                    .select("order_id")
                                   
   val unClosedOrdersFromFileDF=orderFileDF.join(closedOrderIdsDF,Seq("order_id"),"left_anti")
   (closedOrdersDF,unClosedOrdersFromFileDF)
  }
  
  //matchOrders function makes calls to the matchOrdersBetweenOrderBookAndOrderFile and matchOrderWithInTheOrderFile methods using orders from order book and order file
  def matchOrders(readOrderBookDF:DataFrame,ordersDF:DataFrame):(DataFrame,DataFrame)={
    
      val (closedOrdersFromBook,unclosedOrdersFromBook,unclosedOrdersFromFile)=matchOrdersBetweenOrderBookAndOrderFile(readOrderBookDF,ordersDF)
      val (closedOrdersFromFile,unclosedOrdersFromFileFinal)=matchOrderWithInTheOrderFile(unclosedOrdersFromFile)
      val closedOrdersDF=closedOrdersFromBook.union(closedOrdersFromFile)
      val unClosedOrdersFinalDF=unclosedOrdersFromBook.union(unclosedOrdersFromFileFinal)
      
      (closedOrdersDF,unClosedOrdersFinalDF)

    
  }
  
  //mergeFiles merges multiple files present in the source path into a single file in destination path
  def mergeFiles(src:String,dest:String)={
    if(fs.exists(new Path(src)))
    {
    FileUtil.copyMerge(fs,new Path(src),fs,new Path(dest),true,spark.sparkContext.hadoopConfiguration,null)
    }
    else 
      println(s"$src File doesnot exists.Nothing to archive")
  }

  def main(args:Array[String])={
    
    try{
      val ordersFilePath=args(0)
      val ordersFileName=args(1)
      val orderBookFilePath=args(2)
      val orderBookFileName=args(3)
      val closedOrdersFilePath=args(4)
      val closeOrdersFileName=args(5)
      val orderFileArchivalPath=args(6)
      val orderBookArchivalPath=args(7)
     
      val ordersSchema=StructType(Array(
          StructField("order_id",StringType),
          StructField("user_name",StringType),
          StructField("order_time",LongType),
          StructField("order_type",StringType),
          StructField("quantity",IntegerType),
          StructField("price",LongType)
      ))

      val ordersDF=readCsvFile(s"$ordersFilePath/$ordersFileName",ordersSchema).persist()
      val orderBookDF=readCsvFile(s"$orderBookFilePath/$orderBookFileName",ordersSchema).persist()    
      val (closedOrders,unclosedOrders)=matchOrders(orderBookDF,ordersDF)
      unclosedOrders.coalesce(1).write.format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat").mode(SaveMode.Append).save(s"$orderBookFilePath/temp_${orderBookFileName}")
      closedOrders.coalesce(1).write.format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat").mode(SaveMode.Append).save(s"$closedOrdersFilePath/temp_${closeOrdersFileName}")
      archiveFile(s"$ordersFilePath/$ordersFileName",s"$orderFileArchivalPath/$ordersFileName".replace(".csv", s"_$ts.csv"))
      archiveFile(s"$orderBookFilePath/$orderBookFileName",s"$orderBookArchivalPath/$orderBookFileName".replace(".csv", s"_$ts.csv"))
      mergeFiles(s"$orderBookFilePath/temp_${orderBookFileName}",s"$orderBookFilePath/${orderBookFileName}")
      mergeFiles(s"$closedOrdersFilePath/temp_${closeOrdersFileName}",s"$closedOrdersFilePath/${closeOrdersFileName}".replace(".csv", s"_$ts.csv"))
      println(s"Processing Completed.\nTradeMatch output can be found in this path $closedOrdersFilePath ")
    }
    catch {
      case e:Throwable=>e.printStackTrace()
    }
    finally{
      spark.stop()
    }
  }
}
