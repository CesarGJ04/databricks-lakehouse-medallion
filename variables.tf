variable "location" {
  type    = string
  default = "eastus"
}
variable "tags" {
  type = map(string)
  default = {
    project = "SalesStore"
    owner   = "team-data"
  }
}