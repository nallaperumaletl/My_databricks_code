import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.view(
    name = 'trans_flight' 
)
def trans_flight():

    df = spark.readStream.format("delta")\
        .load('/Volumes/workspace/0_flight_bronze/bronzevolume/flights/data/')
    df1 = df.withColumn('flight_date', to_date(col('flight_date')))\
        .withColumn('modified_date', current_timestamp())\
        .drop('_rescued_data')
    return df1

dlt.create_streaming_table('silver_flights')

dlt.create_auto_cdc_flow(
    target = 'silver_flights',
    source = 'trans_flight',
    keys = ['flight_id'],
    stored_as_scd_type = 1
)