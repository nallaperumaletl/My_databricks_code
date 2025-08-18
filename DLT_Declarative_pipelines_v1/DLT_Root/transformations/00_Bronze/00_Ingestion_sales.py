import dlt

# sales Expectations
sales_rules = {
  "rule_1" : "sales_id IS NOT NULL"
}

# Empty streaming table (append flow from East and west dataset)
# Here, instead of @dlt for expectations on top of def we defined inside the append streaming function.
dlt.create_streaming_table(
  name="sales_stg",
  expect_all_or_fail=sales_rules
)

# Creating East sales flow:
@dlt.append_flow(target="sales_stg")
def east_sales():
  df = spark.readStream.table("dlt_dev.source_raw.sales_east")
  return df

# Creating west sales flow:
@dlt.append_flow(target="sales_stg")
def west_sales():
    df = spark.readStream.table("dlt_dev.source_raw.sales_west")
    return df