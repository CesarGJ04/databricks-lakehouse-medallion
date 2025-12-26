output "resource_group_name" {
  value = module.rg.resource_group_name
}

output "storage_account_name" {
  value = module.adls.storage_account_name
}

output "connector_principal_id" {
  value = module.access_connector.principal_id
}

