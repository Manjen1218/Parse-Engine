import os
import glob
import json
import pandas as pd

# 1. Collect all JSON paths
json_paths = glob.glob("sku_setting/*/*.json")

sku_data = {}       # { sku: { field: json_object } }
field_dbtypes = {}  # { field: dbtype }

# 2. Load and parse each JSON
for path in json_paths:
    sku = os.path.splitext(os.path.basename(path))[0]

    with open(path, 'r') as f:
        data = json.load(f)

    sku_data[sku] = data

    for field, meta in data.items():
        # Expecting dbtype from meta dict
        dbtype = meta.get("dbtype", "UNKNOWN")
        if field not in field_dbtypes:
            field_dbtypes[field] = dbtype
        # Optional: warn if conflicting dbtypes
        elif field_dbtypes[field] != dbtype:
            print(f"⚠️  Warning: conflicting dbtypes for '{field}': "
                  f"'{field_dbtypes[field]}' vs '{dbtype}'")

# 3. Prepare unique sorted field list and SKU list
all_fields = sorted(field_dbtypes.keys())
all_skus = sorted(sku_data.keys())

# 4. Build output rows
rows = []
for field in all_fields:
    row = {
        "column_name": field,
        "dbtype": field_dbtypes[field]
    }
    for sku in all_skus:
        row[sku] = field if field in sku_data[sku] else None
    rows.append(row)

# 5. Write to CSV
df = pd.DataFrame(rows)
df.to_csv("sku_fields.csv", index=False)
