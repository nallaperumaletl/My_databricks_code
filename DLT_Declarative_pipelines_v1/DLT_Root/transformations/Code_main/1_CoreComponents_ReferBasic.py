# import dlt
# from utilities import utils
# from pyspark.sql.functions import col

# ########################################################################################################
# # Creating Streaming table:
# ########################################################################################################
# @dlt.table(
#     name="first_streaming_table"
# )
# def first_streaming_table():

#     df = spark.readStream.table("dlt_dev.source_raw.order_test").withColumn("Cust_id_manupulation",utils.cust_id(col("customer_id")))
#     return df
# ########################################################################################################
# # Creating Materialized Views
# # For Batch Processing: Materialized Views( o/p of the data query will be physically stored in database)

# # spark.read.table => Materialized view
# # spark.readStream.table => Streaming table
# ########################################################################################################

# @dlt.table(
#     name="first_materialized_view"
# )
# def first_materialized_view():
#     df = spark.read.table("dlt_dev.source_raw.order_test").withColumn("mat_cust_id_manupulation",utils.cust_id(col("customer_id")))
#     return df

# ########################################################################################################
# # Creating Normal view: Just holds only the query.

# @dlt.view(
#     name="first_batch_view"
# )
# def first_batch_view():
#     df = spark.read.table("dlt_dev.source_raw.order_test").withColumn("batch_cust_id_manupulation",utils.cust_id(col("customer_id")))
#     return df

# ########################################################################################################
# # Creating Streaming view: Just holds only the query.

# @dlt.view(
#     name="first_streaming_view"
# )
# def first_streaming_view():
#     df = spark.readStream.table("dlt_dev.source_raw.order_test").withColumn("streaming_cust_id_manupulation",utils.cust_id(col("customer_id")))
#     return df

