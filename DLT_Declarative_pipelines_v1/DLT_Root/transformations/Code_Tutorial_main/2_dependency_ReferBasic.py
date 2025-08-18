# # Creating an end-2-end Basic Pipeline

# import dlt
# from pyspark.sql.functions import *

# # Staging Area:

# @dlt.table(
#     name="staging_orders"
# )
# def staging_orders():
#   df = spark.readStream.table("dlt_dev.source_raw.order_test")
#   return df


# # Creating transformed view
# @dlt.view(
#     name = "transformed_orders"
# )
# def transformed_orders():
# #   df = dlt.read("staging_orders")
# # OR
#   df = spark.readStream.table("staging_orders")
#   df = df.withColumn("order_status",lower(col("order_status")))
#   return df

# # Creating Agg/Business view:

# @dlt.table(
#     name = "aggregated_orders"
# )
# def aggregated_orders():
#     df = dlt.readStream("transformed_orders")
#     df = df.groupBy("order_status").count()
#     return df

# # DBTITLE 1,Let's check the tables in the Databricks Lakehouse
# display(spark.sql("SHOW TABLES"))