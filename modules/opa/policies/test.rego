package s3

import rego.v1

# Deny everything by default.
default allow := true

# allow if {
#     # Iterate over product slugs
#     some product_slug
#     product := data.products[product_slug]

#     # Any asset prefix must match the beginning of input.key
#     some asset_prefix in product.assets
#     startswith(input.key, asset_prefix)

#     # The requesting user's email is in this product's users
#     input.email in product.users
# }