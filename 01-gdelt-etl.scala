// Databricks notebook source
println("Starting Cluster")

// COMMAND ----------

// MAGIC %md
// MAGIC ### GDELT File scraping
// MAGIC 1. Scrape the links of relevant files
// MAGIC 2. Send download requests
// MAGIC 3. Store downloaded files in local file store (EC2 Instance's file system) -> Will look for a way for bash to save file straight into DBFS
// MAGIC 
// MAGIC 
// MAGIC Note to self
// MAGIC - Variable ```spark``` refers to the SparkSession

// COMMAND ----------

// MAGIC %md
// MAGIC Creating temp folder to store downloaded zipped files

// COMMAND ----------

// MAGIC %sh
// MAGIC if [[ -e /tmp/gdelt ]] ; then
// MAGIC   rm -rf /tmp/gdelt
// MAGIC fi
// MAGIC mkdir /tmp/gdelt

// COMMAND ----------

// MAGIC %md
// MAGIC Creating another temp folder to store unzipped files

// COMMAND ----------

// MAGIC %sh
// MAGIC if [[ -e /tmp/gdelt_unzipped ]] ; then
// MAGIC   rm -rf /tmp/gdelt_unzipped
// MAGIC fi
// MAGIC mkdir /tmp/gdelt_unzipped

// COMMAND ----------

// MAGIC %md
// MAGIC Loop to download zipped files

// COMMAND ----------

// MAGIC %sh
// MAGIC 
// MAGIC MASTER_URL=http://data.gdeltproject.org/gkg/md5sums
// MAGIC 
// MAGIC echo "Retrieving files"
// MAGIC 
// MAGIC FILES=`curl ${MASTER_URL} 2>/dev/null | awk '{print $2}' | grep gkg.csv.zip | grep '202107\|202106\|202105'`
// MAGIC for FILE in $FILES; do
// MAGIC   FILE_URL="http://data.gdeltproject.org/gkg/$FILE"
// MAGIC   echo "Downloading ${FILE_URL}"
// MAGIC   wget $FILE_URL -O /tmp/gdelt/$FILE > /dev/null 2>&1
// MAGIC   unzip /tmp/gdelt/$FILE -d /tmp/gdelt_unzipped > /dev/null 2>&1
// MAGIC   rm -rf /tmp/gdelt/$FILE
// MAGIC done

// COMMAND ----------

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

// Raw read - schema-less
val rawGdeltData = spark.read.format("csv").option("header", "true").option("delimiter", "\t").load("file:/tmp/gdelt_unzipped/*.csv")

// Bronze DataFrame - imposed Schema
val bronzeGdeltData = rawGdeltData
  .withColumn("date", col("DATE").cast(IntegerType))
  .withColumn("organisations", col("ORGANIZATIONS").cast(StringType))
  .withColumn("themes", col("THEMES").cast(StringType))
  .withColumn("locations", col("LOCATIONS").cast(StringType))
  .withColumn("tone", element_at(split(col("TONE"), ","), 1).cast(FloatType))
  .withColumn("sources", col("SOURCES").cast(StringType))
  .withColumn("urls", col("SOURCEURLS").cast(StringType))
  .filter("organisations IS NOT NULL AND themes IS NOT NULL") // Filter out null organisations
  .select(
    col("date"),
    col("organisations"),
    col("themes"),
    col("locations"),
    col("tone"),
    col("sources"),
    col("urls")
  )

// COMMAND ----------

// MAGIC %sql
// MAGIC -- Loading the checkpoint
// MAGIC CREATE TABLE default.gdelt_bronze
// MAGIC USING DELTA
// MAGIC LOCATION '/user/hive/warehouse/gdelt_bronze'

// COMMAND ----------

// Append Bronze table to Delta table
bronzeGdeltData.write.format("delta").mode("append").saveAsTable("default.gdelt_bronze")

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT *
// MAGIC FROM default.gdelt_bronze
// MAGIC -- VERSION AS OF 3
// MAGIC WHERE date < 20210701
// MAGIC LIMIT 5

// COMMAND ----------

// MAGIC %md
// MAGIC ### Streaming

// COMMAND ----------

import org.apache.spark.sql.functions._

// Writing a function to select only ESG themes
// I.e. THEMES relating to ECON, ENV or SOC related
def themeSwitch(x: String) = {
  if (x != null) {
    x.split("_").head match { // Getting the theme prefix e.g. ECON_DEREGULATION -> ECON
    // Pattern matching and switch to transform
    case "ENV" => Some("E")
    case "ECON" => Some("G")
    case "SOC" => Some("S")
    case _ => None: Option[String]
    }
  }
  else {
    None: Option[String]
  }
} 

// Wrapping the above function in a UDF
val filterThemes = udf((xs: String) => {
  xs.split(";").flatMap(x => themeSwitch(x)).distinct
})

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger

// Perhaps a Stream is not needed here, writing a Delta table in append mode would probably been sufficient, but I wanted to give it a go anyway.
val silverGdeltData = spark
  .readStream
  .format("delta")
  .table("default.gdelt_bronze")
  .withColumn("esg_themes", filterThemes(col("themes")))
  .filter(size(col("esg_themes")) > 0)
  .withColumn("organisation", explode(split(col("organisations"), ";")))
  .select(
    to_date(col("date").cast(StringType), "yyyyMMdd").as("date"),
    col("organisation"),
    col("esg_themes"),
    col("locations"),
    col("tone"),
    col("sources"),
    col("urls")
  )

silverGdeltData
  .writeStream
  .outputMode(outputMode="append")
  .trigger(Trigger.Once) // The query will execute only one micro-batch to process all the available data and then stop on its own.
  .option("checkpointLocation", "/tmp/gdelt_checkpoint")
  .format("delta")
  .start("/dbfs/user/hive/warehouse/gdelt_silver") // Delta table as a data sink

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE TABLE IF NOT EXISTS default.gdelt_silver
// MAGIC USING DELTA
// MAGIC LOCATION '/dbfs/user/hive/warehouse/gdelt_silver';
// MAGIC 
// MAGIC SELECT *
// MAGIC FROM default.gdelt_silver
// MAGIC LIMIT 5;

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT COUNT(*)
// MAGIC FROM default.gdelt_silver

// COMMAND ----------

import org.apache.spark.sql.functions._

// Getting the Gold table
val goldGdeltData = spark
  .read
  .format("delta")
  .load("/dbfs/user/hive/warehouse/gdelt_silver") // Reading from silver as input i.e. input silver -> output gold
  .withColumn("esg_theme", explode(col("esg_themes")))  
  .groupBy("date", "organisation", "esg_theme")
  .agg(
    sum("tone").as("tone"),
    count("*").as("total")
  )
  .select(
    col("date"),
    col("organisation"),
    col("esg_theme"),
    col("tone"),
    col("total")
  )

// COMMAND ----------

// Writing Gold Gdelt table to Delta
// Since this one is an aggregated table (Where there are sum of tone and Count(*)) --> It must be overwritten each time
goldGdeltData.write.format("delta").mode("overwrite").saveAsTable("default.gdelt_gold")

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT *
// MAGIC FROM default.gdelt_gold
// MAGIC LIMIT 5

// COMMAND ----------

// MAGIC %md
// MAGIC ### Graph analytics
// MAGIC 
// MAGIC 1. Nodes: organisations 
// MAGIC 2. Edges / Relationships: companies relating to each other by appearing in the same news article, whereby the article is identified by the URL
// MAGIC 
// MAGIC #### Relationship will be as follows (Cypher):
// MAGIC (c1: Organisation)-[r:RELATE {rel_count, avg_tone}]-(c2: Organisation)

// COMMAND ----------

// UDF to build combinations of organisations
val buildTuples = udf((orgs: Seq[String]) => {
  orgs.flatMap(x1 => {
    orgs.map(x2 => {
      (x1, x2)
    })
  }).toSeq.filter({ case (x1, x2) => 
    x1 != x2 // No self edges
  })
})

// COMMAND ----------

// Nodes and Edges DataFrames
val nodes = spark
  .read
  .format("delta")
  .load("/dbfs/user/hive/warehouse/gdelt_silver")
  .groupBy(col("organisation"))
  .agg(avg(col("tone")).as("tone"))
  .select(
    col("organisation").as("organisation"),
    col("tone")
  )
  .distinct().toDF()

// Staging table for edges - since this one requires heavy processing
val edgesStaging = spark
  .read
  .format("delta")
  .load("/dbfs/user/hive/warehouse/gdelt_silver")
  .groupBy("urls", "tone")
  .agg(collect_list(col("organisation")).as("organisations"))
  .withColumn("tuples", buildTuples(col("organisations")))
  .select(
    col("urls"),
    col("tone"),
    col("tuples")
  )

// COMMAND ----------

// Write to Delta table
edgesStaging.write.format("delta").mode("overwrite").saveAsTable("default.gdelt_graph_staging_edges")

// COMMAND ----------

// Displaying nodes DataFrame
display(nodes)

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT *
// MAGIC FROM default.gdelt_graph_staging_edges

// COMMAND ----------

// Real edges table
val edges = spark.table("default.gdelt_graph_staging_edges")
  .withColumn("tuple", explode(col("tuples")))
  .withColumn("src", col("tuple._1"))
  .withColumn("dst", col("tuple._2"))
  .groupBy("src", "dst")
  .agg(
    sum(lit(1)).as("relationship"),
    avg(col("tone")).as("avg_tone")
  )
  .filter(col("relationship") > 100)
  .toDF()

// COMMAND ----------

// Writing edges table - Overwrite mode because this is an aggregated data table
edges.write.format("delta").mode("overwrite").saveAsTable("default.gdelt_graph_edges")

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT *
// MAGIC FROM default.gdelt_graph_edges
