

**About the Project:**
  This project contains the  spark scala code solution for Refinitiv Matching Engine Exercise with the below problem statement
   Refinitiv Matching Engine Exercise

		Your task is to create a new matching engine for FX orders. The engine will take a CSV file of orders for a given
		currency pair and match orders together. In this example you'll be looking at USD/GBP.
		
		There are two types of orders, BUY and SELL orders. A BUY order is for the price in USD you'll pay for GBP, SELL
		order is the price in USD you'll sell GBP for.
		
		Each order has the following fields:
		1. Order ID
		        - This is a unique ID in the file which is used to track an order
		2. User Name
		        - This is the user name of the user making the order
		3. Order Time
		        - This is the time, in milliseconds since Jan 1st 1970, the order was placed
		4. Order Type
		        - Either BUY or SELL
		5. Quantity
		        - The number of currency units you want to BUY or SELL
		6. Price
		        - The price you wish to sell for, this is in the lowest unit of the currency, i.e. for GBP it's in pence and for USD it's cents
		
		The matching engine must do the following:
		- It should match orders when they have the same quantity
		- If an order is matched it should be closed
		- If an order is not matched it should be kept on an "order book" and wait for an order which does match
		- When matching an order to the book the order should look for the best price
		- The best price for a BUY is the lowest possible price to BUY for
		- The best price for a SELL is the highest possible price to SELL for
		- You should always use the price from the "order book" when matching orders
		- When an order has matched you should record the IDs of both orders, the price, quantity and time of the match
		- If two orders match with the same price the first order is used
        - Orders won't have the same timestamp
  

**Build Tool:** Maven

**Dependencies Used :** org.scala-lang:scala-library:2.11.11,
                   org.apache.hadoop:hadoop-common:2.10.0,
                   org.apache.spark:spark-sql_2.11:2.4.3
		   
**Build command:** mvn clean package

**UberJarName :** TradeExecutionProgram-1.0.0-SNAPSHOT-jar-with-dependencies.jar
**ThinJarName:** TradeExecutionProgram-1.0.0-SNAPSHOT.jar
**Input Arguments:**
            This program takes 7 mandatory arguments as below                                          
             1.ordersFilePath - File path where the order files are present     
             2.Order Filename - Name of the orderFile                        
             3.orderBookFilePath - File path where the Booking will be Stored                                                                                         
             4.orderBookFileName - Order Book File name to be created                                     
             5.closedOrdersFilePath - Output file path where the closed/matched orders will be stored                                                                                         
             6.closeOrdersFileName - Output file name                                                     
             7.orderFileArchivalPath - Path where the input/Order files will be archived after processing 
             8.orderBookArchivalPath - Path where the orderBook files will be archived after processing


**Calling Syntax:** 

1.Sample Command To Run Application Locally on 8 cores with uber jar:

      spark-submit --name "TradeExecEngine" --master local[8] --class com.excercise.code.TradeExecution  /home/vagrant/codepractice/jars/TradeExecutionProgram-1.0.0-SNAPSHOT-jar-with-dependencies.jar /home/vagrant/codepractice/input exampleOrders.csv /home/vagrant/codepractice/orderbook orderbook.csv /home/vagrant/codepractice/output tradematch.csv /home/vagrant/codepractice/archive/orderFiles  /home/vagrant/codepractice/archive/orderBooks
      
2.Sample Command To Run Application on yarn with uber jar:

       spark-submit --name "TradeExecEngine" --master yarn --deploy-mode cluster --driver-memory 1g --executor-memory 1g --executor-cores 4 --class com.excercise.code.TradeExecution  /home/vagrant/codepractice/jars/TradeExecutionProgram-1.0.0-SNAPSHOT-jar-with-dependencies.jar /home/vagrant/codepractice/input exampleOrders.csv /home/vagrant/codepractice/orderbook orderbook.csv /home/vagrant/codepractice/output tradematch.csv /home/vagrant/codepractice/archive/orderFiles  /home/vagrant/codepractice/archive/orderBooks

3.Sample Command to Run with Thin jar:

    spark-submit --name "TradeExecEngine" --master yarn --deploy-mode cluster --driver-memory 1g --executor-memory 1g --executor-cores 4 --packages org.scala-lang:scala-library:2.11.11,org.apache.hadoop:hadoop-common:2.10.0,org.apache.spark:spark-core_2.11:2.4.3,org.apache.spark:spark-sql_2.11:2.4.3 --class com.excercise.code.TradeExecution  /home/vagrant/codepractice/jars/TradeExecutionProgram-1.0.0-SNAPSHOT.jar /home/vagrant/codepractice/input exampleOrders.csv /home/vagrant/codepractice/orderbook orderbook.csv /home/vagrant/codepractice/output tradematch.csv /home/vagrant/codepractice/archive/orderFiles  /home/vagrant/codepractice/archive/orderBooks


 


**Prerequisites for Running the Application:**
1.JAVA8 Should be installed

2.Spark 2.4.3 or greater should be installed

3.Hadoop 2.10 or Higher should be installed (If running on windows then Winutils.exe should be configured to emulate Hadoop on windows)
 

**Psuedo code:**
Step-1 : Import the required libraries and packages.

Step-2 : Define an object named "TradeExecution" for the main program.

Step-3 : Create a Spark session and configure it.

Step-4 : Define a function named "readCsvFile" that takes a file path and schema as input and reads the CSV file into a DataFrame using the provided schema.

Step-5 : Define a function named "archiveFile" that moves a file from the source path to the destination path.

Step-6 : Set up variables for the current timestamp using SimpleDateFormat and Calendar.

Step-7 : Define a function named "matchOrdersBetweenOrderBookAndOrderFile" that takes the order book DataFrame and order file DataFrame as input and performs matching between the two datasets to find closed and unclosed orders.

Inside the "matchOrdersBetweenOrderBookAndOrderFile" function:

		a. Extract Buy orders From order Book and Rank the buy orders in the order book on the quantity and order time 
		   Eg:Rank() Over(Partition By quantity order By orderTime Asc) 
		   
		b. Extract Sell orders From order Book and Rank the buy orders in the order book on the quantity and order time
		   Eg:Rank() Over(Partition By quantity order By orderTime Asc)
		   
		c. Extract Buy orders From order File and Rank the buy orders in the order File on the quantity ,  order price  and order time 
		   Eg:Rank() Over(Partition By quantity order By orderPrice Desc ,orderTime Asc) 
		   
		d. Extract Sell orders From order File and Rank the buy orders in the order book on the quantity,  order price  and order time 
		   Eg:Rank() Over(Partition By quantity order By orderPrice Asc ,orderTime Asc) 
		   
		e. Match the buy orders from the order book and  sell orders from order File based on order quantity and rank 
		
		f. Match the sell orders from the order book and  buy orders from order File based on order quantity and rank 
		
		h. Create a DataFrame of closed orders by combining the matched buy and sell orders.
		
		i. Extract the closed Order Ids from the previous step result
		
		j. Filter the order book and order file DataFrames to get unclosed orders by excluding the closed order IDs.
		
		k. Return the closed orders DataFrame and unclosed order DataFrames.
		
Step-8 : Define a function named "matchOrderWithInTheOrderFile" that takes the order file DataFrame as input and performs matching within the order file dataset to find closed and unclosed orders.

		Inside the "matchOrderWithInTheOrderFile" function:
		
		a. Rank the buy orders in the order file DataFrame based on quantity and order time.
		
		b. Rank the sell orders in the order file DataFrame based on quantity and order time.
		
		c. Match the buy and sell orders within the order file based on quantity and rank.
		
		d. Create a DataFrame of closed order IDs by combining the matched buy and sell orders.
		
		e. Filter the order file DataFrame to get unclosed orders by excluding the closed order IDs.
		
		f. Return the closed order DataFrame and unclosed order DataFrame.
		
Step-9 : Define a function named "matchOrders" that takes the order book DataFrame and order DataFrame as input and performs matching between the two datasets.
Inside the "matchOrders" function:

		a. Call the "matchOrdersBetweenOrderBookAndOrderFile" function to get the closed and unclosed orders from the order book and order file datasets.
		
		b. Call the "matchOrderWithInTheOrderFile" function to get additional closed and unclosed orders within the order file dataset.
		
		c. Combine the closed order DataFrames from the order book and order file datasets.
		
		d. Combine the unclosed order DataFrames from the order book and order file datasets.
		
		e. Return the combined closed order DataFrame and combined unclosed order DataFrame.
		
Step-10: Define a function named "mergeFiles" that takes the source and destination paths
Inside the "mergeFiles" function:

                a.check if the source path exists .
		
		b.If the source path exists then merge Files present in the source path to Destination path using FileUtil.copyMerge utility
		
Step-11 : Define the "main" function that takes command-line arguments as input.

		Inside the "main" function:
		
		a. Parse the command-line arguments for file paths and names.
		
		b. Define the schema for the orders DataFrame.
		
		c. Read the orders DataFrame and order book DataFrame using the "readCsvFile" function.
		
		d. Call the "matchOrders" function to get the closed and unclosed orders.
		
		e. Write the unclosed orders and closed orders to temporary files.
		
		f. Archive the original orders file to the specified archival path.
		
		g. Archive the order Book file to the specified archival path.
		
		h. Merge the unclosed Orders file to the order Book path received from the command line argument to create a single orderBook file
		
		i. Merge the closed Orders file to the closedOrdersFilePath received from the command line argument with the given naming pattern.



**Methods Used in this Program:**
1.readCsvFile(filePath:String,Schema:StructType):DataFrame :

           1.Takes filepath and schema as arguments 
	   
           2.Reads the csv file and returns a dataframe with file content
           
2.archiveFile(sourcePath:String,destinationPath:String) :

            1.Takes sourcePath and destinationPath as arguments
	    
            2.Moves the file from sourcePath to destination path
            
3.matchOrdersBetweenOrderBookAndOrderFile(orderBookDF:DataFrame,orderFileDF:DataFrame):(DataFrame,DataFrame,DataFrame) :

            1.Takes order data from OrderBook and OrderFile as two seperate dataframe type arguments
	    
            2.Splits the OrderBookDF  based on OrderType and ranks them on quantity and ordertime.Then stores the result into 2 seperate dataframes orderBookBuyOrdersRankedDF,orderBookSellOrdersRankedDF 
	    
            3.Splits the orderFileDF into  orderBookBuyOrdersRankedDF,orderBookSellOrdersRankedDF based on OrderType .
	    
            4.Ranks the sellorders from orderFileDF on quantity and orderPrice ascending order ,then stores the result into orderFileSellOrderRankedDF 
	    
            5.Ranks the buyorders from orderFileDF on quantity and orderprice descending order,then stores the result into orderFileBuyOrderRankedDF
	    
            6.Matches the orderBookBuyOrdersRankedDF and orderFileSellOrderRankedDF on quantity and rank to identify the best price match for the buyOrders present in the book(i.e lowest sell price from the order file for the matching quantity).
	    
              Then stores the result into orderBookBuyOrdersMatched.
            7.Matches the orderBookSellOrdersRankedDF and orderFileBuyOrderRankedDF on quantity and rank to identify the best price match for the sellOrders present in the book(i.e highest buy price from the order file for the matching quantity)
              Then stores the result into orderBookSellOrdersMatched.
	      
            8.Combines the orderBookBuyOrdersMatched and orderBookSellOrdersMatched and stores the result into orderBookFileOrdersMatched .
	    
            9.Identifies the closed OrderIds from the orderBookFileOrdersMatched dataframe and stores the result into closedOrderIdsDF
	    
            10.Filters out the order records from orderBookDF and orderFileDF whose orderIds does not match with closedOrderIdsDF.
	    
            11.Stores the step 10 result into unClosedOrdersFromBook and unClosedOrdersFromFile
	    
            12.Finally it returns 3 dataframes orderBookFileOrdersMatched,unClosedOrdersFromBook,unClosedOrdersFromFile 

4.matchOrderWithInTheOrderFile(orderFileDF:DataFrame):(DataFrame,DataFrame) :

            1.Takes the orderFileDF (orders from the orderFile) as an arguement
	    
            2.Splits the orderFileDF on orderType and ranks them on quantity and ordertime .Then stores the result into 2 separate dataframes buyOrdersRankedDF,sellOrdersRankedDF
	    
            3.Match buyOrdersRankedDF and sellOrdersRankedDF on orderquantity and rank to identify the matching records based on first come first serve priority and store the result into closedOrders
	    
            4.closedOrderIdsDF from closedOrders
	    
            5.Filters out the order records from  orderFileDF whose orderId is not present in the closedOrderIds, and stores the result in unClosedOrdersFromFile
	    
            6.Returns a tuple (closedOrders,unClosedOrdersFromFile)
            
5.matchOrders(readOrderBook:DataFrame,ordersDF:DataFrame):(DataFrame,DataFrame) :

            1.Takes readOrderBook dataframe containing the ordersfrom orderbook and   ordersDF dataframe containing the orders from orderfile/input file as arguments
            2.calls the matchOrdersBetweenOrderBookAndOrderFile  using readOrderBook and ordersDF.Then captures the result into closedOrdersFromBook ,unclosedOrdersFromBook,unclosedOrdersFromFile dataframes.
	    
                  closedOrdersFromBook --- Matched orders from orderBook to orderFile
		  
                  unclosedOrdersFromBook --- unclosed orders from orderBook 
		  
                  unclosedOrdersFromFile --- unclosed orders from orderFile
		  
            3.calls the matchOrderWithInTheOrderFile using unclosedOrdersFromFile and captures the result into closedOrdersFromFile , unclosedOrdersFromFileFinal
	    
                  closedOrdersFromFile --- Matched orders from the orderfile, which are left over after matching with orderBook  
		  
                  unclosedOrdersFromFileFinal --- Left over orders from the orderfile , after performing the match with orderbook and itself.  
		  
            4.combines the unclosedOrdersFromBook and unclosedOrdersFromFileFinal  into unClosedOrdersFinalDF
	    
                  unClosedOrdersFinalDF -- This contains the unclosed orders from the book and unclosed orders from the file.
                                           This serves as the orderbook for the next run
					   
            5.Returns a tuple containing closedOrdersDF and unClosedOrdersFinalDF
                  
                  
 6.mergeFiles(src:String,dest:String) :
 
           1. Takes source path(src) and destination path(dest) as arguments 
	   
           2. Merges the files present into the src to dest and then removes the src path
        

 
**Method Execution Sequence:**
 Main -> readCsvFile  ->matchOrders->archiveFile ->mergeFiles 

 
**Scenarios Handled in this code:**

 1.Single OrderMatch with in the file (Exactly 1 matching quantity between Buy and Sell Orders)
 
 2.Multiple OrdersMatch with in the file (1 Or more Buy/Sell records matches with 1 or more Sell/Buy records from the file. Priority will be given on first come first serve basis while matching the records).
 
 3.Single Buy/Sell Match from Order Book to the Single Sell/Buy Match from input File
 
 4.When a match occurs in both the Order Book and Input File it self, then Order Book will be given the priority
 
 5.When multiple matches occurs from Order Book to the Input File then priority will be assigned on First Come First Serve Basis
 
 6.When a Single Order from Order Book Matches with multiple Orders in Input file then priority will be given on the basis of Best Buy Price and Best Sell Price from Order Book Records Perspective    
 
**Sample Input and Output:**
 
 **Example 1:**
 
 Input :
 
 exampleOrders.csv:
 
        1,Steve,1623239770,SELL,72,1550
	2,Ben,1623239771,BUY,8,148
	3,Steve,1623239772,SELL,24,6612
	4,Kim,1623239773,SELL,98,435
	5,Sarah,1623239774,BUY,72,5039
	6,Ben,1623239775,SELL,75,6356
	7,Kim,1623239776,BUY,38,7957
	8,Alex,1623239777,BUY,51,218
	9,Jennifer,1623239778,SELL,29,204
	10,Alicia,1623239779,BUY,89,7596
	11,Alex,1623239780,BUY,70,2351
	12,James,1623239781,SELL,89,4352
	13,Sarah,1623239782,SELL,98,8302
	14,Alicia,1623239783,BUY,56,8771
	15,Alex,1623239784,BUY,83,737
	16,Andrew,1623239785,SELL,15,61
	17,Steve,1623239786,BUY,62,4381
	18,Ben,1623239787,BUY,33,5843
	19,Alicia,1623239788,BUY,20,5255
	20,James,1623239789,SELL,68,4260
  
  output:
  closedOrders (tradematch_20230612182400.csv):
  
		    5,1,1623239774,72,1550
		    12,10,1623239781,89,7596
		    
  orderBook (orderbook.csv) :
  
                        2,Ben,1623239771,BUY,8,148
			3,Steve,1623239772,SELL,24,6612
			4,Kim,1623239773,SELL,98,435
			6,Ben,1623239775,SELL,75,6356
			7,Kim,1623239776,BUY,38,7957
			8,Alex,1623239777,BUY,51,218
			9,Jennifer,1623239778,SELL,29,204
			11,Alex,1623239780,BUY,70,2351
			13,Sarah,1623239782,SELL,98,8302
			14,Alicia,1623239783,BUY,56,8771
			15,Alex,1623239784,BUY,83,737
			16,Andrew,1623239785,SELL,15,61
			17,Steve,1623239786,BUY,62,4381
			18,Ben,1623239787,BUY,33,5843
			19,Alicia,1623239788,BUY,20,5255
			20,James,1623239789,SELL,68,4260
			
			
**Example2:**
    Input  :
    
    orderFile (exampleOrders.csv):
    
                21,Jane,1623239790,BUY,98,8312
		22,Chris,1623239792,BUY,98,8300
		23,Willium,1623239793,SELL,38,8000
		24,Christina,1623239794,BUY,100,9000
		25,Sravan,1623239795,BUY,38,8500
		26,Arun,1623239796,SELL,38,8600
		27,Kiran,1623239796,SELL,38,8650
		
    orderBook( orderBook.csv from the example1):
    
                        2,Ben,1623239771,BUY,8,148
			3,Steve,1623239772,SELL,24,6612
			4,Kim,1623239773,SELL,98,435
			6,Ben,1623239775,SELL,75,6356
			7,Kim,1623239776,BUY,38,7957
			8,Alex,1623239777,BUY,51,218
			9,Jennifer,1623239778,SELL,29,204
			11,Alex,1623239780,BUY,70,2351
			13,Sarah,1623239782,SELL,98,8302
			14,Alicia,1623239783,BUY,56,8771
			15,Alex,1623239784,BUY,83,737
			16,Andrew,1623239785,SELL,15,61
			17,Steve,1623239786,BUY,62,4381
			18,Ben,1623239787,BUY,33,5843
			19,Alicia,1623239788,BUY,20,5255
			20,James,1623239789,SELL,68,4260
       
    output :
    
        closedOrders (tradematch_20230612202054.csv) :

	                23,7,1623239793,38,7957
			21,4,1623239790,98,435
			22,13,1623239792,98,8302
			26,25,1623239796,38,8500
			
		orderBook(orderbook.csv) :
		
		        2,Ben,1623239771,BUY,8,148
			3,Steve,1623239772,SELL,24,6612
			6,Ben,1623239775,SELL,75,6356
			8,Alex,1623239777,BUY,51,218
			9,Jennifer,1623239778,SELL,29,204
			11,Alex,1623239780,BUY,70,2351
			14,Alicia,1623239783,BUY,56,8771
			15,Alex,1623239784,BUY,83,737
			16,Andrew,1623239785,SELL,15,61
			17,Steve,1623239786,BUY,62,4381
			18,Ben,1623239787,BUY,33,5843
			19,Alicia,1623239788,BUY,20,5255
			20,James,1623239789,SELL,68,4260
			24,Christina,1623239794,BUY,100,9000
			27,Kiran,1623239796,SELL,38,8650
        
			
            		    
		    
		    
    
    	
      
 
 
 
