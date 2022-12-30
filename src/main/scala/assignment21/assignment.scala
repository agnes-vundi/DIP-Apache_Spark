package assignment21

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{window, column, desc, col}


import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Column
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, IntegerType, DoubleType}
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.{count, sum, min, max, asc, desc, udf, to_date, avg}

import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.array
import org.apache.spark.sql.SparkSession

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}




import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.{KMeans, KMeansSummary}
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.ml.feature.StringIndexer


import java.io.{PrintWriter, File}


//import java.lang.Thread
import sys.process._


import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection.immutable.Range

object assignment  {
  // Suppress the log messages:
  Logger.getLogger("org").setLevel(Level.OFF)
                       
  
  val spark = SparkSession.builder()
                          .appName("assignment")
                          .config("spark.driver.host", "localhost")
                          .master("local")
                          .getOrCreate()
                          
                          
  //spark.conf.set("spark.sql.shuffle.partitions", "5")
                          
  // Reading data : File "data/dataK5D2.csv" & "data/dataK5D3.csv"
  // contains medical data of patients for a hospital 
  
  // Defining a schema for the dataK5D2 i.e to indicate the corresponding columns datatypes
  val customSchema = new StructType(Array(
      new StructField("a", DoubleType, true),
      new StructField("b", DoubleType, true),
      new StructField("LABEL", StringType, true)))
                          
  val dataK5D2 = spark.read
                      .option("header", "true")
                      .option("delimiter", ",")
                      .schema(customSchema)
                      .csv("data/dataK5D2.csv")
  
  // Caching the dataframe to avoid multiple loading of the data this increases runtime performance
  dataK5D2.cache()                    
  dataK5D2.printSchema()
                      
  // Defining schema for the second data as its 3-dimensional
  val schema_1 = StructType(Array(
      StructField("a", DoubleType, true),
      StructField("b", DoubleType, true),
      StructField("c", DoubleType, true),
      StructField("LABEL", StringType, true)))
                       
  val dataK5D3 =  spark.read
                       .option("header", true)
                       .option("delimiter", ",")
                       .schema(schema_1)
                       .csv("data/dataK5D3.csv")
  
  dataK5D3.cache()    // cached the training data to improve performance             
  dataK5D3.printSchema()                     
  
  def task1(df: DataFrame, k: Int): Array[(Double, Double)] = {
    val assembler = new VectorAssembler()
                        .setInputCols(Array("a", "b"))
                        .setOutputCol("features")
    
    // Scale the features column to a range [min, max]. This increases model efficiency
    val scaler = new MinMaxScaler()
        .setInputCol("features")
        .setOutputCol("ScaledFeatures")
        
    // The pipeline takes a sequence of all features transfomers algorithims and performs them in stages on the dataframe
    // The stages are executed in the order stated that is vectorAssembler then MinMaxScaler
    // This helps in streamlining processing work flow.
        
    val transformationPipeline = new Pipeline()
        .setStages(Array(assembler, scaler))
        
    // Pipeline fitted to the dataframe to produce a transformer
    // transformer.transform() transforms the input data in the order of stages called.
    val pipeline = transformationPipeline.fit(df)
    val transformedData = pipeline.transform(df)
    
    // Initializing the K-Means model
    val kmeans = new KMeans().setFeaturesCol("ScaledFeatures")
        .setK(k)
        .setSeed(1L)
        .setMaxIter(20)
    
   // Trains the k-means model with the transformed dataFrame
    val model = kmeans.fit(transformedData)
   
   // Showing the cluster centers results
    val centers = model.clusterCenters.map(s=> (s(0), s(1))).toArray
    
    centers.foreach(println)

    return centers
    
    
  }

  def task2(df: DataFrame, k: Int): Array[(Double, Double, Double)] = {
    val assembler = new VectorAssembler()
                        .setInputCols(Array("a", "b", "c"))
                        .setOutputCol("features")
    
    //Scales the input feature vector column to a range [min,max]
    val scaler = new MinMaxScaler()
                    .setInputCol("features")
                    .setOutputCol("scaled_features")
                    
    //The pipeline takes in the feature transformers vectorAssembler and MinMaxScaler in that order
    //and performs them in stages. The Fit() method produces a pipeline transformer on the data.
    //Pipeline helps in improving processing flow of the machine learning algorithms.
    val transformerPipeline = new Pipeline()
                                  .setStages(Array(assembler, scaler))
                                  
    // Fit() method to produce a transformer on the data frame columns
    val pipeline = transformerPipeline.fit(df)
    
    // Applying the transformer on the data frame to create the vectorized and scaled feature column
    val transformedDF = pipeline.transform(df)
    
    // Initializing the K-Means model
    val kmeans = new KMeans().setK(k).setSeed(1L).setMaxIter(20).setFeaturesCol("scaled_features")
    
    // Training the model 
    val kmodel = kmeans.fit(transformedDF)
    
    // Getting the cluster centers results
    val clusters = kmodel.clusterCenters.map(vec => (vec(0), vec(1), vec(2))).toArray
    
    clusters.foreach(println)
    
    return clusters
    
  }
  
  // Creates a new column with the labels as numeric values
  import org.apache.spark.sql.functions._
  import spark.implicits._
  
  val dataK5D3WithLabels = dataK5D2.withColumn("newLABEL", when(col("LABEL") === "Fatal", 0.0).otherwise(1.0))
  
  def task3(df: DataFrame, k: Int): Array[(Double, Double)] = {
    //maps the input columns to a features vector array
    val assembler = new VectorAssembler()
                        .setInputCols(Array("a", "b", "newLABEL"))
                        .setOutputCol("features")
    
    
    // Scale the features column to a range [min, max]. This increases model efficiency
    /*val scaler = new MinMaxScaler()
        .setInputCol("features")
        .setOutputCol("ScaledFeatures")*/
        
        
    // The pipeline takes a sequence of all features transfomers algorithims and performs them in stages on the dataframe
    // The stages are executed in the order stated that is vectorAssembler, MinMaxScaler 
    // This helps in streamlining processing work flow.
        
    val transformationPipeline = new Pipeline()
        .setStages(Array(assembler))
        
    // Pipeline fitted to the data frame to produce a transformer
    // transformer.transform() transforms the input data in the order of stages called.
    val pipeline = transformationPipeline.fit(df)
    val transformedData = pipeline.transform(df)
        
    // Initializing the K-Means model
    val kmeans = new KMeans().setK(k).setSeed(1L).setMaxIter(20).setFeaturesCol("features")
    
    // Training the model 
    val kmodel = kmeans.fit(transformedData)
    
    // Getting the cluster centers results   
    val clusters = kmodel.clusterCenters.map(vec => (vec(0), vec(1), vec(2)))
                                        .sortBy(c => c._3)
                                        .take(2)
                                        
    val fatalClusters = clusters.map(s => (s._1, s._2))
    
    fatalClusters.foreach(println)
    
    return fatalClusters
        
        
  }

  
  // Parameter low is the lowest k and high is the highest one.
  def task4(df: DataFrame, low: Int, high: Int): Array[(Int, Double)]  = {
    val assembler = new VectorAssembler()
                        .setInputCols(Array("a", "b"))
                        .setOutputCol("features")
    
    // Scale the features column to a range [min, max]. This increases model efficiency
    val scaler = new MinMaxScaler()
        .setInputCol("features")
        .setOutputCol("ScaledFeatures")
    
    val transformationPipeline = new Pipeline()
        .setStages(Array(assembler, scaler))
        
    // Pipeline fitted to the dataframe to produce a transformer
    // transformer.transform() transforms the input data in the order of stages called.
    val pipeline = transformationPipeline.fit(df)
    val transformedData = pipeline.transform(df)
    
    // Initializing and training the K-Means model
    
    def costCalculator(df: DataFrame, k: Int): Double={
      val kmeans = new KMeans().setFeaturesCol("ScaledFeatures")
        .setK(k)
        .setSeed(1L)
        .setMaxIter(20)
      val kmodel = kmeans.fit(df)
      // Evaluate clustering by computing Within Set Sum of Squared Errors.
      val cost = kmodel.computeCost(df)
      
      return cost
      
    }
    
    // The range of k values
    val kvalues = low to high      
    val KCostPairs = kvalues.map(kval => (kval, costCalculator(transformedData, kval))).toArray
   
    return KCostPairs
    
    
  }
     
  
    
}


