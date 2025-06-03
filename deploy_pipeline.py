import dlt
import requests

BASE_URL = "https://jaffle-shop.scalevector.ai/api/v1"

def get_paginated_data(endpoint: str):
    url = f"{BASE_URL}/{endpoint}"
    page = 1
    while True:
        response = requests.get(url, params={'page': page})
        response.raise_for_status()
        data = response.json()
        yield data
        if not data or len(data) == 0:
            break
        page += 1

@dlt.resource(table_name="customers", write_disposition="merge", primary_key="id")
def get_customers():
    for page in get_paginated_data('customers'):
        yield page

@dlt.source
def jaffle_shop_source():
    return get_customers

if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="jaffle_shop",
        destination="duckdb",
        dataset_name="jaffle_shop_data"
    )
    load_info = pipeline.run(jaffle_shop_source())
    print(pipeline.last_trace.last_normalize_info)
    print(load_info) 