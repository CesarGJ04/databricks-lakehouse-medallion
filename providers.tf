terraform {
  required_version = ">= 1.6.0" #mi version de terraform mayor a 1.6.0 

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm" #el provider oficial de AureRM de HashiCorp
      version = "~> 4.43.0"
    }
  }
}

provider "azurerm" { #se indica que usare Azure RM
  features {}

  storage_use_azuread = true
}
# Mejor si lo pasas por variable (subscription_id), si tmpc queda el ID de Azure CLI automaticamente











#Azure RM (Azure Resource Manager) es el servicio de implementación y administración de Microsoft
#Azure que permite crear, actualizar y eliminar recursos de forma coherente y repetible usando 
#plantillas declarativas (Infraestructura como Código), además de aplicar control de acceso, 
#bloqueos y etiquetas para organizar y proteger esos recursos. Es la capa de administración 
#fundamental en Azure, agrupando recursos en grupos de recursos para gestionarlos juntos como una unidad lógica. 


#🔄 Orden de resolución de credenciales del provider azurerm
#Valores explícitos en el provider (NO tienes ninguno) → ❌
#Variables de entorno ARM_* → si no existen → ❌
#Azure CLI (az login) → ✔ SE USA
#Managed Identity (si estás en Azure) → no aplica local