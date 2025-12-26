variable "identity_name" {
  type = string
}

variable "resource_group_name" {
  type = string
}

variable "location" {
  type = string
}

# scope_resource_id: por ejemplo azurerm_storage_account.this.id
variable "scope_resource_id" {
  type = string
}

variable "tags" {
  type    = map(string)
  default = {}
}
