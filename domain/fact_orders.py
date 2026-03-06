def create_fact_orders(orders_df):

    fact_df = orders_df.select(
        "order_id",
        "customer_id",
        "product_id",
        "payment_id",
        "amount"
    )

    return fact_df