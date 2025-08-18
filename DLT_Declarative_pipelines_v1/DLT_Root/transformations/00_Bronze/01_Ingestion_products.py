import dlt


# Products Expectations:
products_rule = {
    "rule_1" : "product_id IS NOT NULL",
    "rule_2" : "price >= 0"
}

# Ingesting Products
@dlt.table(
    name="products_stg",
    comment="Products table"
)
@dlt.expect_all(products_rule)
def products_stg():
    df =  spark.readStream.table("dlt_dev.source_raw.products")
    return df