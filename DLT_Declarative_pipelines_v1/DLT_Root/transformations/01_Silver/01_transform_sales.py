import dlt
from pyspark.sql.functions import col

# Documentation:
# https://learn.microsoft.com/en-us/azure/databricks/dlt-ref/dlt-python-ref-apply-changes


# Transforming sales data:
@dlt.view(
    name = "sales_stg_enrich_trans_v"
)
def sales_stg_trans_v():
    df = spark.readStream.table("sales_stg")
    df = df.withColumn("Total_amount", col("quantity") * col("amount"))
    return df

# creating destination silver table
dlt.create_streaming_table(
    name = "sales_enrich"
)
import dlt

dlt.create_auto_cdc_flow(
  target = "sales_enrich",
  source = "sales_stg_enrich_trans_v",
  keys = ["sales_id"],
  sequence_by = "sale_timestamp",
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
