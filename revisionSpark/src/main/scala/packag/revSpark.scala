package packag

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object revSpark {
  case class schema(txnno:String
                   ,txndate:String
                   ,custno:String
                   ,amount:String
                   ,category:String
                   ,product:String
                   ,city:String
                   ,state:String
                   ,spendby:String)
                   
  def main(args:Array[String]):Unit={
    println("STARTED")
    
    val conf= new SparkConf().setAppName("First").setMaster("local[*]")
    val context=new SparkContext(conf)
    context.setLogLevel("ERROR")
    val spark= SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    
  val  unifiedlist =List("txnno","txndate","custno","amount","category","product","city","state","spendby" )
    
  
  
     println
     println("=========== 1=>Creation of Int List and iterate=============")
    println
    val listint= List(1,4,6,7)
    val addlist= listint.map(x=>x+2)
         addlist.foreach(println)
    println
     
    println("=========== 2=>Creation of String List and filter contains zeyo=============")
    println
    val liststr= List("zeyobron","analytics","zeyo")  
    liststr.foreach(println)
    println
    val filterlist=liststr.filter(x=>x.contains("zeyo"))
        filterlist.foreach(println)
        
    println
    println("=========== 3=>Read file1 as an RDD=============")
    println 
        val readRdd= context.textFile("file:///D:/allData/revdata/file1.txt")
        readRdd.take(5).foreach(println)
        println
        println("Filter Gymnastics Rows")
        println
        val filterrdd=readRdd.filter(x=>x.contains("Gymnastics"))
        filterrdd.take(5).foreach(println)
        
    println
    println("=========== 4=> Case Class,Impose  Schema RDD and filter=============")
    println 
    
    val splitmap=readRdd.map(x=>x.split(","))
    val schemardd=splitmap.map(x=>schema(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))
    val filterRdd=schemardd.filter(x=>x.product.contains("Gymnastics"))
        filterRdd.take(5).foreach(println)
   
   println
   println("=========== Raw Data=============")
   println
   
   val readdata=context.textFile("file:///D:/allData/revdata/file2.txt")
   readdata.take(5).foreach(println)
   
   println
   println("============5=>Convert to ROW RDD============")
   println
   val splitrdd=readdata.map(x=>x.split(","))
   val rowrdd=splitrdd.map(x=>Row(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))
   rowrdd.take(5).foreach(println)
   
   println
   println("============6=>Create Dataframe using Schema RDD============")
    println
    
    val schemadf=schemardd.toDF().select(unifiedlist.map(col):_*) 
    schemadf.show(5)
    
    println
   println("============6=>Create Dataframe using Row RDD============")
    println
    val simpleSchema = StructType(Array(
                                        StructField("txnno",StringType,true),
                                        StructField("txndate",StringType,true),
                                        StructField("custno",StringType,true),
                                        StructField("amount", StringType, true),
                                        StructField("category", StringType, true),
                                        StructField("product", StringType, true),
                                        StructField("city", StringType, true),
                                        StructField("state", StringType, true),
                                        StructField("spendby", StringType, true)
                                         ))
                                         
    val rowdf=spark.createDataFrame(rowrdd, simpleSchema).select(unifiedlist.map(col):_*)                          
       rowdf.show(5)
       println
       println("============7=>Read file as csv with header true============")
       println
       
       
    val csvread=spark.read.format("csv").option("header", "true").load("file:///D:/allData/revdata/file3.txt")
                                                                 .select(unifiedlist.map(col):_*) 
       csvread.show(5)
       println
       println("============8=>Read file as JSON ============")
       println
       
       
       val jsonread=spark.read.format("json").option("header", "true").load("file:///D:/allData/revdata/file4.json")
                                                                      .select(unifiedlist.map(col):_*) 
       jsonread.show(5)
       
       println
       println("============8=>Read file as  PARQUET============")
       println
       
       val parqread=spark.read.option("header", "true").load("file:///D:/allData/revdata/file5.parquet")
                                                       .select(unifiedlist.map(col):_*) 
       parqread.show(5)
       
       println
       println("============9=>Read file as  XML============")
       println
       
       val xmlread=spark.read.format("xml").option("rowtag", "txndata").load("file:///D:/allData/revdata/file6")
                                                                        .select(unifiedlist.map(col):_*) 
       xmlread.show(5)
       
       println
       println("============10=>Union all DataFrame============")
       println
       
       val uniondf=schemadf.union(rowdf).union(csvread).union(jsonread).union(parqread).union(xmlread)
                   uniondf.show(5)
       
       println
       println("============11=>Rename txndate as year and split and add another column as status and filter============")
       println
       
       val dslspark=uniondf.withColumn("txndate", expr("split(txndate,'-')[2]"))
                            .withColumnRenamed("txndate", "year")
                            .withColumn("status", expr("case when spendby='cash' then 1 else 0 end"))
                            .filter("txnno>500")
                dslspark.show(5)    
                
       println
       println("============12=>Aggregation and GroupBy============")
       println         
       val aggSum=uniondf.groupBy("category").agg(sum("amount").alias("total_rec"))
                  aggSum.show(10)
                  
       println
       println("============13=>Write Avro, Append and do partition============")
       println             
       
       val writeavro=aggSum.write.format("avro").mode("append").partitionBy("category").save("file:///D:/allData/revData/avro")
               println("=============Written as Avro==============")
               
       println
       println("============14=>Complex Data ============")
       println             
       
       val compjson=spark.read.format("json").option("multiline", "true").load("file:///D:/allData/address.json")
            compjson.show()
            compjson.printSchema()
            
            println()
            println("==========14=>Flatten=========")
        val flattenjson=compjson.withColumn("phone_numbers", explode(col("phone_numbers")).alias("phone_numbers"))    
                                .select(
                                    
                                      col("age"),
                                      col("billing_address.*"),
                                      col("date_of_birth"),
                                      col("email_address"),
                                      col("first_name"),
                                      col("height_cm"),
                                      col("is_alive"),
                                      col("last_name"),
                                      col("phone_numbers.*"),
                                      col("shipping_address.*")
                                
                                       )
                           flattenjson.persist()           
                           flattenjson.show()
                           flattenjson.printSchema()
  }
}