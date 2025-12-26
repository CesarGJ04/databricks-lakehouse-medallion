# Storage Account (ADLS Gen2)
resource "azurerm_storage_account" "this" {
  name                = var.storage_account_name # ej. adlssalesstore
  resource_group_name = var.resource_group_name
  location            = var.location

  account_tier             = var.account_tier
  account_replication_type = var.account_replication_type
  account_kind             = "StorageV2"

  # ADLS Gen2
  is_hns_enabled                  = true
  default_to_oauth_authentication = true
  shared_access_key_enabled       = false


  tags = var.tags
}

resource "time_sleep" "wait_for_adls" {
  depends_on = [azurerm_storage_account.this]
  create_duration = "60s"
}

# Create the ADLS Gen2 filesystem  (equivalente a container pero para dfs)
resource "azurerm_storage_data_lake_gen2_filesystem" "this" {
  name               = var.filesystem_name # salesxstore
  storage_account_id = azurerm_storage_account.this.id

  depends_on = [time_sleep.wait_for_adls]
}

# Create directories inside the filesystem
resource "azurerm_storage_data_lake_gen2_path" "paths" {
  for_each = toset(var.paths) # ["bronze", "silver", ...]

  storage_account_id = azurerm_storage_account.this.id
  filesystem_name    = azurerm_storage_data_lake_gen2_filesystem.this.name

  path     = each.key
  resource = "directory"

  depends_on = [
    azurerm_storage_data_lake_gen2_filesystem.this
  ]
}

#my unique endpoint:
#abfss://salesxstore@adlssalesstore.dfs.core.windows.net/