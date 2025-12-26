output "storage_account_id" {
  value = azurerm_storage_account.this.id
}

output "storage_account_name" {
  value = azurerm_storage_account.this.name
}

output "filesystem_name" {
  value = azurerm_storage_data_lake_gen2_filesystem.this.name
}

output "paths" {
  value = [for p in azurerm_storage_data_lake_gen2_path.paths : p.path]
}

output "dfs_endpoint" {
  value = azurerm_storage_account.this.primary_dfs_endpoint
}