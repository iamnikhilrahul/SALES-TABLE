# Databricks notebook source
# MAGIC %md
# MAGIC ##DATA LAKE TO ADB##
# MAGIC

# COMMAND ----------

storage_account_name = "datanikh"
container_name = "source"
sas_token = "*************************"
if any(mount.mountPoint == f"/mnt/{container_name}" for mount in dbutils.fs.mounts()):
    dbutils.fs.unmount(f"/mnt/{container_name}")
    
dbutils.fs.mount(
    source=f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net",
    mount_point=f"/mnt/{container_name}",
    extra_configs={f"fs.azure.sas.{container_name}.{storage_account_name}.blob.core.windows.net": sas_token}
)

# COMMAND ----------

dbutils.fs.ls("/mnt/source")

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

sales = spark.read.format("csv").option("header", "true")\
    .option("inferSchema", "true").load("/mnt/source/fact_sales.csv")
#sales.display()
sales.rdd.getNumPartitions()
#sales = sales.repartition(6)
sales.withColumn("partition_id", spark_partition_id()).groupBy("partition_id").count().show()
	
 

# COMMAND ----------

customer = spark.read.format("csv").option("header", "true")\
    .option("inferSchema", "true").load("/mnt/source/dim_customer.csv")
#customer.display()
customer.rdd.getNumPartitions()
customer = customer.repartition(6)
customer.withColumn("partition_id", spark_partition_id()).groupBy("partition_id").count().show()


# COMMAND ----------

from pyspark.sql.types import *
schema = StructType([
    StructField("summary", IntegerType(), True),        
    StructField("Date_ID", StringType(), True),       
    StructField("Date", IntegerType(), True),
    StructField("Year", IntegerType(), True),
    StructField("Month", IntegerType(), True),       
    StructField("Quarter", IntegerType(), True),
    StructField("Weekday", IntegerType(), True)  
])
date = spark.read.format("csv").option("header", "true")\
    .schema(schema).load("/mnt/source/dim_date.csv")
#date.display()
date.display()


# COMMAND ----------

product = spark.read.format("csv").option("header", "true")\
    .option("inferSchema", "true").load("/mnt/source/dim_product.csv")
product.rdd.getNumPartitions()
product = product.coalesce(1)
product.withColumn("partition_id", spark_partition_id()).groupBy("partition_id").count().show()


# COMMAND ----------

store = spark.read.format("csv").option("header", "true")\
    .option("inferSchema", "true").load("/mnt/source/dim_store.csv")
#store.display()
store.rdd.getNumPartitions()
#store = store.repartition(6)
store.withColumn("partition_id", spark_partition_id()).groupBy("partition_id").count().show()


# COMMAND ----------

inventory = spark.read.format("csv").option("header", "true")\
    .option("inferSchema", "true").load("/mnt/source/fact_inventory.csv")
#inventory.display()
inventory.rdd.getNumPartitions()
#inventory = inventory.repartition(6)
inventory.withColumn("partition_id", spark_partition_id()).groupBy("partition_id").count().show()


# COMMAND ----------

returns = spark.read.format("csv").option("header", "true")\
    .option("inferSchema", "true").load("/mnt/source/fact_returns.csv")
#returns.display()
returns.rdd.getNumPartitions()
#returns = returns.repartition(6)
#returns.withColumn("partition_id", spark_partition_id()).groupBy("partition_id").count().show()
returns.display()
	

# COMMAND ----------

sales_report = sales.select("customer_id", "product_id","Date_ID","Total_Sale_Amount")
#sales_report.display()
product.describe()
sales_report = sales_report.alias("s").join(product.alias("p"), sales_report['product_id'] == product['product_id'], "inner").selectExpr("s.customer_id", "s.product_id","s.date_id","s.Total_Sale_Amount","p.Category as product_Category")


# COMMAND ----------

  
    sales_report = sales_report.alias("s").join(customer.alias("c") , sales_report['customer_id'] == customer['Customer_ID'], "inner").withColumn("age_group",when(col("Age")<25,"Young").otherwise("Adult")).select("s.customer_id", "s.product_id", "s.date_id","s.Total_Sale_Amount", "s.product_Category","age_group")

# COMMAND ----------

from pyspark.sql.window import *
win = Window.partitionBy("product_id","date_id")
sales_report = sales_report.withColumn("total_revenue", sum(col("Total_Sale_Amount")).over(win))
sales_report = sales_report.alias("s").join(returns.alias("r"), (sales_report['customer_id'] == returns['Customer_ID']) & (sales_report['date_id'] == returns['Return_Date_ID']), "leftouter").select("s.customer_id", "s.product_id", "s.date_id","s.Total_Sale_Amount", "s.product_Category","s.age_group","s.total_revenue","r.Return_Amount")
sales_report = sales_report.withColumn("total_return", sum(col("Return_Amount")).over(win))
sales_report = sales_report.drop("Return_Amount","Total_Sale_Amount")

# COMMAND ----------

sales_report.display()

# COMMAND ----------

sales_report.write.mode("overwrite").format("delta").save("/mnt/sink/sales_report")

# COMMAND ----------

