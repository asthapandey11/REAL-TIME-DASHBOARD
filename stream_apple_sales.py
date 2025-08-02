import pandas as pd
import json
import time

df = pd.read_csv('Apple Sales Data India.csv')

records = df.to_dict(orient='records')

print(f"Loaded {len(records)} Apple sales records. Starting sequential streaming with accumulation...")

for i, sale in enumerate(records):
    try:
        with open('sales_data.json', 'r') as f:
            data = json.load(f)
    except FileNotFoundError:
        data = []

    data.append(sale)

    with open('sales_data.json', 'w') as f:
        json.dump(data, f, indent=4, default=str)

    print(f"Record {i+1}/{len(records)} streamed: Order ID {sale['Order ID']} | Purchase Date: {sale['Purchase Date']}")

    time.sleep(1)

print("All records streamed (accumulating).")
