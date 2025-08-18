import dlt
from pyspark.sql.functions import col, cast, upper
from pyspark.sql.types import IntegerType

# Documentation:
# https://learn.microsoft.com/en-us/azure/databricks/dlt-ref/dlt-python-ref-apply-changes


# Transforming customers data:
@dlt.view(
    name = "customers_stg_enrich_trans_v"
)
def sales_stg_trans_v():
    df = spark.readStream.table("customers_stg")
    df = df.withColumn("customer_name", upper(col("customer_name")))
    return df

# creating destination silver table
dlt.create_streaming_table(
    name = "customers_enrich"
)
import dlt

dlt.create_auto_cdc_flow(
  target = "customers_enrich",
  source = "customers_stg_enrich_trans_v",
  keys = ["customer_id"],
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
