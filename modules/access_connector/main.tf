resource "azurerm_databricks_access_connector" "this" {
  name                = var.identity_name # sales_store_connection
  resource_group_name = var.resource_group_name
  location            = var.location

  identity {
    type = "SystemAssigned"
  }

  tags = var.tags
}

resource "azurerm_role_assignment" "storage_blob_data_contributor" {
  scope                = var.scope_resource_id # storage account id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.this.identity[0].principal_id
}
