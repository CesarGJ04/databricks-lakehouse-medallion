output "principal_id" {
  value = azurerm_databricks_access_connector.this.identity[0].principal_id
}

output "access_connector_id" {
  value = azurerm_databricks_access_connector.this.id
}

