from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


@udf(returnType=StringType())
def cust_id(customer_id):
    """customer_id manupulation"""
    return customer_id * 100
