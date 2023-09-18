// Databricks notebook source
// DBTITLE 1,Import Snowflake Utilities
// MAGIC %run "/Zeus Team/Database Tools/Snowflake Utils"

// COMMAND ----------

// DBTITLE 1,Import Snowflake Shared View, Write to Mounted Directory then Move to Production Staging Directory
import java.time.LocalDateTime
import java.text.SimpleDateFormat

dbutils.widgets.text("database_name", "")
dbutils.widgets.text("view_name", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("customer_key", "")

val databaseName = dbutils.widgets.get("database_name")
val viewName = dbutils.widgets.get("view_name")
val customerKey = dbutils.widgets.get("customer_key")
val schema = dbutils.widgets.get("schema")

def moveFilesFromDirectoryToTarget(stagingDir: String, destinationDir: String): Unit = {
  val staging = dbutils.fs.ls(s"${stagingDir}").map(_.path).filter(item => item.endsWith(".csv/"))
  val stagingMap = staging.map{file => dbutils.fs.ls(file).map(_.path).filter(item => item.endsWith(".csv"))}.flatten
  stagingMap.foreach { stagedFile =>
    val filePath = stagedFile.replace("dbfs:/","")
    val fileName = filePath.split("/")(4)
    val singleFileLoc = s"${destinationDir}/${fileName}"
    dbutils.fs.mv(filePath, s"${singleFileLoc}", true)
    dbutils.fs.rm(stagingDir, true) 
    println(s"Final transfer \nfrom: ${filePath}  \nto: ${singleFileLoc}\n")
  }
}

def writeCorewelview(databaseName: String, viewName: String, schema: String, customerKey: String) {
  
  val snowflakeView = spark.read
    .format("snowflake")
    .option("dbtable", viewName)
    .option("sfUrl", s"${snowflakeConfig.accountName}.${snowflakeConfig.host}")
    .option("sfUser", snowflakeUser)
    .option("sfPassword", snowflakePassword)
    .option("sfDatabase", databaseName)
    .option("sfSchema", schema)
    .option("sfWarehouse", snowflakeConfig.warehouse)
    .load()

  val initialPath = "mnt/snowflake_shares/staging/initial/"
  val path = "/" + initialPath
  val uuid = java.util.UUID.randomUUID.toString
  val timestamp = LocalDateTime.now()
  val month = new SimpleDateFormat("MM").format(timestamp.getMonthValue)
  val monthYear = timestamp.getYear() + "-" + month
  val fileName = s"${viewName}___${customerKey}___${timestamp}.${uuid}.csv"
  val fullFileName = path + fileName
  val desiredPath = s"s3a://artemis-client-etl-data/staging/snowflake_shares/validated/${monthYear}/${fileName}"

  snowflakeView.coalesce(1)
  .write
  .option("header","true")
  .csv(fullFileName)

  val readInDs = spark.read.csv(s"dbfs:${fullFileName}")

  val actualColumnCount = snowflakeView.columns.size
  val readinColumnCount = readInDs.columns.size
  
  if ((actualColumnCount == readinColumnCount) && ((readInDs.count - 1) == snowflakeView.count)){
    println(s"count passed for view: ${viewName} --- now performing write")
    println(s"Validation passed, extract moved to the following final path: ${desiredPath}")

    // pull out single file, rename and cleanup folder
    moveFilesFromDirectoryToTarget(initialPath, "mnt/snowflake_shares/staging")
  }
  
  else {
    throw new RuntimeException(s"Validation failed, actualColumnCount is readinColumnCount: ${readinColumnCount} is: ${readinColumnCount} and readinRowCount is: ${readInDs.count - 1} and snowflake row count is: ${snowflakeView.count} unable to move file extract: ${fullFileName} to the final path")
  }
}

val listViewName = viewName.split(",").map(_.trim).toList

listViewName.foreach {view => 
        writeCorewelview(databaseName, view, schema, customerKey)
}
