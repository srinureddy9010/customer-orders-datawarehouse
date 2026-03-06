from domain.customer_scd2 import apply_scd2
import pandas as pd

def test_customer_history():

    data = pd.DataFrame({
        "customer_id":[1],
        "name":["Ravi"],
        "address":["Hyderabad"]
    })

    result = apply_scd2(data)

    assert "start_date" in result.columns