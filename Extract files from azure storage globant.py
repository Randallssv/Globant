# Databricks notebook source
# DBTITLE 1,Importing libraries
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import LongType, StringType, StructField, StructType, BooleanType, ArrayType, IntegerType, DoubleType, DateType, DecimalType , TimestampType

# COMMAND ----------

# DBTITLE 1,Configuración al data lake
spark.conf.set(
  "fs.azure.account.key.stgpocsandboxrsv.blob.core.windows.net",
  "QWMsgMJxtKV1aJwvkh0oxjPHpJD3kzpHkZZKkSCMlYvMkL02gp3JpvcUpvELrNRL6EnLipsd9l75+ASt8gO0Ow==")

# COMMAND ----------

# DBTITLE 1,Mount data lake storage account in a temp DB
# MAGIC %sql
# MAGIC DROP DATABASE import CASCADE;
# MAGIC CREATE DATABASE IF NOT EXISTS import;
# MAGIC DROP TABLE if exists import.rawData;
# MAGIC CREATE TABLE import.rawData 
# MAGIC USING csv
# MAGIC LOCATION "wasbs://files-landing@stgpocsandboxrsv.blob.core.windows.net/"
# MAGIC OPTIONS (header "true", inferSchema "true", multiline "true", quote '"', escape '"')

# COMMAND ----------

# DBTITLE 1,Extract data from rawData, text cleanse  & create unique id concat
raw_input = sqlContext.sql("select * from import.rawData")
raw_input.createOrReplaceTempView("raw_data")


raw_input = raw_input.withColumn("product_name", expr("BTRIM(upper(product_name), ' \t')")) \
                     .withColumn("mrp", expr("BTRIM(upper(mrp), ' \t')")) \
                     .withColumn("pdp_url", expr("BTRIM(upper(pdp_url), ' \t')")) \
                     .withColumn("brand_name", expr("BTRIM(upper(brand_name), ' \t')")) \
                     .withColumn("product_category", expr("BTRIM(upper(product_category), ' \t')")) \
                     .withColumn("retailer", expr("BTRIM(upper(retailer), ' \t')")) \
                     .withColumn("description", expr("BTRIM(upper(description), ' \t')")) \
                     .withColumn("total_sizes", expr("BTRIM(upper(total_sizes), ' \t')")) \
                     .withColumn("available_size", expr("BTRIM(upper(available_size), ' \t')")) \
                     .withColumn("color", expr("BTRIM(upper(color), ' \t')"))



#Generate concat to generate a product unique id
#One unique product is defined by the unique combination of product name, MRP, URL, color, total size and the available size with the largest combination of sizes

raw_input = raw_input.withColumn("Unique_id", concat(
    coalesce(regexp_replace(raw_input.product_name, "[^a-zA-Z0-9]", ""), lit('')),
    coalesce(regexp_replace(raw_input.mrp, "[^a-zA-Z0-9]", ""), lit('')),
    coalesce(regexp_replace(raw_input.pdp_url, "[^a-zA-Z0-9]", ""), lit('')),
    coalesce(regexp_replace(raw_input.color, "[^a-zA-Z0-9]", ""), lit('')
            )))

#Split the datasets to unnest the necessary arrays and keep the attributes in a dim table

Total_sizes = raw_input.select('Unique_id', 'total_sizes').distinct()

Available_sizes = raw_input.select('Unique_id', 'available_size').distinct()

Attributes_table = raw_input.select('Unique_id', 'product_name', 'mrp', 'pdp_url', 'brand_name', 'product_category', 'retailer', 'description', 'color').distinct()

# COMMAND ----------

#raw_input.count() #610.782
#Attributes_table.count() #42.236
#Total_sizes.count() #40.271
#Available_sizes.count() #146.857

# COMMAND ----------

# DBTITLE 1,Unnest the arrays for available sizes & total sizes
Total_sizes_exploded = Total_sizes.select('Unique_id',split(col("total_sizes"),",").alias("NameArray")).drop("total_sizes") \
    .withColumn('total_sizes', explode('NameArray')).drop("NameArray") \
    .withColumn('total_sizes', expr("BTRIM(total_sizes, ' \t')")) \
    .distinct()

Available_sizes_exploded = Available_sizes.select('Unique_id',split(col("available_size"),",").alias("NameArray")).drop("available_size") \
    .withColumn('available_size', explode('NameArray')).drop("NameArray") \
    .withColumn('available_size', expr("BTRIM(available_size, ' \t')")) \
    .distinct()

# COMMAND ----------

# DBTITLE 1,Generate an attributes table with the total sizes for each product and a flag to identify when available
#prepare the Unique_id for the join
Attributes_table = Attributes_table.withColumnRenamed('Unique_id', 'Unique_id_drop')

#Join to get the unique ids
Attributes_table_w_sizes = Total_sizes_exploded.join(Attributes_table,Attributes_table.Unique_id_drop ==  Total_sizes_exploded.Unique_id,"leftouter").drop('Unique_id_drop')

#Include the size in the unique_id col
Attributes_table_w_sizes = Attributes_table_w_sizes.withColumn("Unique_id", concat(col('Unique_id'),col('total_sizes')))
Available_sizes_exploded = Available_sizes_exploded.withColumn("Unique_id", concat(col('Unique_id'),col('available_size')))

#prepare the Unique_id for the join
Available_sizes_exploded = Available_sizes_exploded.withColumnRenamed('Unique_id', 'Unique_id_drop')

#Join to get the flags for the available 
Attributes_table_w_sizes_n_availability = Attributes_table_w_sizes.join(Available_sizes_exploded,Attributes_table_w_sizes.Unique_id ==  Available_sizes_exploded.Unique_id_drop,"leftouter").drop('Unique_id_drop')

# COMMAND ----------

# DBTITLE 1,Generate flag to filter by product type (BRAS,OTHERS), Product size att generation & availability flag
Attributes_table_final = Attributes_table_w_sizes_n_availability.withColumnRenamed('total_sizes', 'size').withColumnRenamed('available_size', 'available_flag')

Attributes_table_final = Attributes_table_final.withColumn('available_flag', when(Attributes_table_final.available_flag
                                                                                  .isNull() ,"NOT_AVAILABLE").otherwise("AVAILABLE")) \
                                                .withColumn('Bras_flag', when((expr("BTRIM(size, ' \t')").substr(1, 2)
                                                                              .isin('30','32','34','36','38','40','42','44','46')) |
                                                                              ((expr("BTRIM(size, ' \t')").substr(2, 1) == "X") & 
                                                                               expr("BTRIM(size, ' \t')").substr(1, 1).isin('1','2','3'))
                                                                              ,"BRA") \
                                                            .otherwise("OTHER")) \
                                                .withColumn('substr_size', expr("BTRIM(size, ' \t')").substr(1, 2))

Attributes_table_final = Attributes_table_final.withColumn('IABSA_SIZE', when((col('Bras_flag') == "BRA") &
                                                                               ((expr("BTRIM(size, ' \t')").substr(1, 2).isin('42','44','46')) |
                                                                               (expr("BTRIM(size, ' \t')").substr(2, 1) == "X"))
                                                                               ,"EXTRA_LARGE") \
                                                           .when((col('Bras_flag') == "BRA") & 
                                                                (expr("BTRIM(size, ' \t')").substr(1, 2).isin('38','40'))
                                                                ,"LARGE") \
                                                           .when((col('Bras_flag') == "BRA") & 
                                                                (expr("BTRIM(size, ' \t')").substr(1, 2).isin('34','36'))
                                                                ,"MEDIUM") \
                                                           .when((col('Bras_flag') == "BRA") & 
                                                                (expr("BTRIM(size, ' \t')").substr(1, 2).isin('30','32'))
                                                                ,"SMALL") \
                                                            .otherwise("NA"))
                                                      
Attributes_table_final = Attributes_table_final.select('Unique_id', 'product_name', 'mrp', 'pdp_url', 'brand_name', 'product_category', 'retailer', 'description', 'color', 'size', 'available_flag', 'Bras_flag', 'IABSA_SIZE').filter(Attributes_table_final.Bras_flag == "BRA")

Attributes_table_final.createOrReplaceTempView("Attributes_table_final")

# COMMAND ----------

# DBTITLE 1,Data quality check
# MAGIC %sql
# MAGIC 
# MAGIC SELECT 
# MAGIC IABSA_SIZE
# MAGIC ,available_flag
# MAGIC ,retailer
# MAGIC ,COUNT(*)
# MAGIC FROM Attributes_table_final
# MAGIC WHERE IABSA_SIZE = 'EXTRA_LARGE'
# MAGIC GROUP BY retailer,IABSA_SIZE, available_flag
# MAGIC ORDER BY retailer, available_flag desc

# COMMAND ----------

# DBTITLE 1,Sink data into SQL Server
# Writing of the tables to MSSQL using the internal JDBC
(Attributes_table_final.write.mode('Overwrite')
                .format("jdbc")
                .option("url", "jdbc:sqlserver://sqlpocsandbox.database.windows.net:1433") 
                .option("databaseName", "dataeng_poc")
                .option("dbTable", "rpt_IABSA_bra_review")
                .option("user", "sandboxadmin")
                .option("password", "123Globant!")
                .save())
