import dlt

# Creating Streamin table:

@dlt.table(
    name="first_streaming_table"
)
def first_streaming_table():

    df = spark.readStream.table("dlt_dev.source_raw.orders_test")
    return df