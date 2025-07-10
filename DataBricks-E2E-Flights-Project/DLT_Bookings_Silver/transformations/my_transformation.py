import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Reading the data from bronze layer
@dlt.table(
    name = 'stage_bookings'
)
def stage_bookings():

    df = spark.read.format('delta') \
        .load('/Volumes/workspace/0_flight_bronze/bronzevolume/bookings/data/')
    
    return df

# Transforming the data in bronze layer
@dlt.view(
    name = 'trans_bookings'
)
def trans_bookings():
    
    df = spark.readStream.table('stage_bookings')
    df1 = df.withColumn('amount', col('amount').cast(DoubleType())) \
            .withColumn('modified_date', current_timestamp()) \
            .withColumn('booking_date', to_date(col('booking_date'))) \
            .drop('_rescued_data')
    
    return df1

# Rules while loading the data to silver layer:
#- Rule1: Booking_id should not be null
#- Rule2: Passanger_id should not be null
rules = {
    "rule1" : "booking_id IS NOT NULL",
    "rule2" : "passenger_id IS NOT NULL"
}

# Loading the data to silver layer
@dlt.table(
    name = 'silver_bookings'
)
@dlt.expect_all_or_drop(rules)
def silver_bookings():
    df = spark.readStream.table('trans_bookings')
    return df