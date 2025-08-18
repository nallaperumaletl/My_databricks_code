import dlt
from pyspark.sql.functions import col, cast
from pyspark.sql.types import IntegerType

# Documentation:
# https://learn.microsoft.com/en-us/azure/databricks/dlt-ref/dlt-python-ref-apply-changes


# Transforming products data:
@dlt.view(
    name = "products_stg_enrich_trans_v"
)
def sales_stg_trans_v():
    df = spark.readStream.table("products_stg")
    # df = df.withColumn("price", col("price").cast("integer"))
    # OR
    df = df.withColumn("price", col("price").cast(IntegerType()))
    return df

# creating destination silver table
dlt.create_streaming_table(
    name = "products_enrich"
)
import dlt

dlt.create_auto_cdc_flow(
  target = "products_enrich",
  source = "products_stg_enrich_trans_v",
  keys = ["product_id"],
  sequence_by = "last_updated",
  ignore_null_updates = False,
  apply_as_deletes = None,
  apply_as_truncates = None,
  column_list = None,
  except_column_list = None,
  stored_as_scd_type = 1,
  track_history_column_list = None,
  track_history_except_column_list = None,
  name = None,
  once = False
)


# Creating silver layer for GOLD Layer:
