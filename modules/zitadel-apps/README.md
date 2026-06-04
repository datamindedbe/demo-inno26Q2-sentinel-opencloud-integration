<!-- BEGIN_TF_DOCS -->
## Requirements

| Name | Version |
|------|---------|
| <a name="requirement_terraform"></a> [terraform](#requirement\_terraform) | 1.11.4 |
| <a name="requirement_zitadel"></a> [zitadel](#requirement\_zitadel) | 2.2.0 |

## Providers

| Name | Version |
|------|---------|
| <a name="provider_zitadel"></a> [zitadel](#provider\_zitadel) | 2.2.0 |

## Modules

No modules.

## Resources

| Name | Type |
|------|------|
| [zitadel_project.s3sentinel](https://registry.terraform.io/providers/zitadel/zitadel/2.2.0/docs/resources/project) | resource |
| [zitadel_org.default](https://registry.terraform.io/providers/zitadel/zitadel/2.2.0/docs/data-sources/org) | data source |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| <a name="input_organisation_id"></a> [organisation\_id](#input\_organisation\_id) | The ID of the ZITADEL organisation. | `string` | n/a | yes |

## Outputs

| Name | Description |
|------|-------------|
| <a name="output_s3sentinel_project_id"></a> [s3sentinel\_project\_id](#output\_s3sentinel\_project\_id) | Zitadel project resource ID for s3sentinel. Used as the expected JWT 'aud' claim. |
<!-- END_TF_DOCS -->