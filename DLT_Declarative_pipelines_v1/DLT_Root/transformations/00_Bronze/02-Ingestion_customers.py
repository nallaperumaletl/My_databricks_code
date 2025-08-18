import dlt

# Cutsomers Expectations
customers_rules = {
    "rule_1" : "customer_id IS NOT NULL",
    "rule_2" : "customer_name IS NOT NULL",
    "rule_3" : "region IS NOT NULL"
}

# Ingesting Products
@dlt.table(
    name="customers_stg",
    comment="Customers table"
)
@dlt.expect_all_or_fail(customers_rules)
def customers_stg():
    df =  spark.readStream.table("dlt_dev.source_raw.customers")
    return df