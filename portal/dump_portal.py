import requests
import json

HEADERS = {"x-key": "593e4d68-26f9-4702-9285-f17c9afb80a5"}
session = requests.Session()
session.headers.update(HEADERS)

API_HOST: str = "http://localhost:5050"

def get_users():
    users = session.get(f"{API_HOST}/api/v2/users").json()["users"]
    for u in users:
        del u["global_role"]
        del u["has_seen_tour"]
        del u["admin_expiry"]
        del u["can_become_admin"]
        del u["first_name"]
        del u["last_name"]
        del u["external_id"]
    return sorted(users, key=lambda x: x["email"])

def get_data_product_roles(data_product_id):
    role_assignments = session.get(f"{API_HOST}/api/v2/authz/role_assignments/data_product",
                                   params={"data_product_id": data_product_id}).json()["role_assignments"]
    users = []
    for ra in role_assignments:
        users.append(ra.get("user").get("email"))
    return users


def get_raw_data_products():
    return session.get(f"{API_HOST}/api/v2/data_products").json()["data_products"]

def get_assets(data_product_id, data_product_namespace):
    assets = session.get(f"{API_HOST}/api/v2/data_products/{data_product_id}/technical_assets").json()["technical_assets"]
    res = [f"{data_product_namespace}/private/"]
    for asset in assets:
        platform_id = asset.get("platform_id")
        service_id = asset.get("service_id")

        platform_service = session.get(
            f"{API_HOST}/api/v2/configuration/platforms/{platform_id}/services/{service_id}"
        ).json()
        new_asset = {
            "id": asset.get("namespace"),
            "service": platform_service.get("service").get("name").lower(),
            "config": asset.get("configuration"),
        }
        if new_asset["service"] == "s3":
            res.append(f"{data_product_namespace}/{new_asset["config"]["path"]}/")
    return sorted(res)

def get_data_products(raw_products):
    data_products_export = {}
    for data_product_info in raw_products:
        data_products_export[data_product_info.get("namespace")] = {
            "name": data_product_info.get("name"),
            "users": get_data_product_roles(data_product_info.get("id")),
            "assets": get_assets(data_product_info.get("id"), data_product_info.get("namespace")),
        }
    return data_products_export

def main():
    print("Dumping data products")
    raw_data_products = get_raw_data_products()
    data_products = get_data_products(raw_data_products)
    with open(
            "data.json",
            "w",
            encoding="utf-8",
    ) as f:
        f.write(json.dumps({
            "products": data_products,
            "users": get_users(),
        }))

if __name__ == "__main__":
    main()


#  curl -s -o /dev/null -w "%{http_code}"   -X PUT http://localhost:8080/inno_days_test/first-product/report5.csv   -H "Authorization: Bearer $ADMIN_TOKEN"   --upload-file /tmp/report.csv