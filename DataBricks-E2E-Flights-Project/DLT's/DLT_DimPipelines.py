import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

#########################################
# BOOKINGS DATA
#########################################

# Reading the data from bronze layer: bookings data
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

#########################################
# FLIGHTS DATA
#########################################

# Reading data from bronze layer: flights data
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
    sequence_by = col('modified_date'),
    stored_as_scd_type = 1
)

#########################################
# PASSENGERS/CUSTOMERS DATA
#########################################

@dlt.view(
    name = 'trans_passengers' 
)
def trans_passengers():

    df = spark.readStream.format("delta")\
            .load('/Volumes/workspace/0_flight_bronze/bronzevolume/customers/data/')
    df1 = df.drop('_rescued_data')\
                .withColumn('modified_date', current_timestamp())
    return df1

dlt.create_streaming_table('silver_passengers')

dlt.create_auto_cdc_flow(
    target = 'silver_passengers',
    source = 'trans_passengers',
    keys = ['passenger_id'],
    sequence_by = col('modified_date'),
    stored_as_scd_type = 1
)

#########################################
# AIRPORTS DATA
#########################################

@dlt.view(
    name = 'trans_airports' 
)
def trans_airports():

    df = spark.readStream.format("delta")\
            .load('/Volumes/workspace/0_flight_bronze/bronzevolume/airports/data/')
    df1 = df.drop('_rescued_data')\
                .withColumn('modified_date', current_timestamp())
    return df1

dlt.create_streaming_table('silver_airports')

dlt.create_auto_cdc_flow(
    target = 'silver_airports',
    source = 'trans_airports',
    keys = ['airport_id'],
    sequence_by = col('modified_date'),
    stored_as_scd_type = 1
)



#########################################
# Silver Business table:
#########################################

@dlt.table(
    name = 'silver_business_view'
)
def silver_business_view():
    df = dlt.readStream('silver_bookings')\
            .join(dlt.readStream('silver_passengers'), ['passenger_id'])\
            .join(dlt.readStream('silver_flights'), ['flight_id'])\
            .join(dlt.readStream('silver_airports'), ['airport_id'])\
            .drop('modified_date')
    return df

###########################################################
#  EXTRA
#  Materialized Business view: (read instead of readStream)
###########################################################˙

@dlt.table(
    name = 'silver_business_materialized_view'
)

def silver_business_materialized_view():
    df = dlt.read('silver_business_view')
    return df
