from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Optimize_Iceberg").getOrCreate()

try:
    spark.sql("ALTER TABLE local_catalog.tiki_silver.products SET TBLPROPERTIES ('write.update.mode'='merge-on-read', 'write.merge.mode'='merge-on-read', 'write.delete.mode'='merge-on-read')")
    print("SUCCESS: Enabled Merge-On-Read for Silver Active")
except Exception as e:
    print("Error:", e)
