import random

NR_PRODUCTS = 30

products = []


for prod_cnt in range(NR_PRODUCTS):
    prod_name = f"product-{prod_cnt}"
    asset_count = random.randint(0, 10)

    prod = {
        "name": prod_name,
        "assets": [f"{prod_name}/asset-{i}/" for i in range(1, asset_count + 1)]+[f"{prod_name}/private/"],
    }

    products.append(prod)

# print(products)

for prod in products:
    read_assets = set()
    for cnt in range(0, 3):
        read_prod = products[random.randint(0, NR_PRODUCTS-1)]
        if len(read_prod["assets"]) > 3:
            read_assets = read_assets.union(random.sample(read_prod["assets"], random.randint(0, 3)))
    prod["read_assets"] = list(read_assets)

import json
json.dump(products, open("new_world.json", "w"))