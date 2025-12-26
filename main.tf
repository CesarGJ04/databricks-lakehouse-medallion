module "rg" {
  source   = "./modules/rg"
  name     = "SalesStore"
  location = var.location
  tags     = var.tags
}

module "adls" {
  source               = "./modules/adls"
  resource_group_name  = module.rg.resource_group_name
  location             = var.location
  storage_account_name = "adlssalesstore" # como pediste
  filesystem_name      = "salesxstore"
  paths                = ["bronze", "silver", "gold", "landing", "logging"]
  tags                 = var.tags
  depends_on           = [module.rg]
}

module "access_connector" {
  source              = "./modules/access_connector"
  identity_name       = "sales_store_conecction"
  resource_group_name = module.rg.resource_group_name
  location            = var.location
  scope_resource_id   = module.adls.storage_account_id
  tags                = var.tags
  depends_on = [
    module.adls
  ]
}

# README.md
# Este módulo queda reservado para Unity Catalog
# (configurado manualmente en Databricks)

# El módulo de Unity Catalog depende de que existan el workspace de Databricks,
# Uso /uc para el root de Unity Catalog (best practice)