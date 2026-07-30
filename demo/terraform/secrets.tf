variable "infisical_project_id" {
  type        = string
  description = "Infisical project ID"
}

variable "infisical_env_slug" {
  type        = string
  description = "Infisical environment slug"
  default     = "prod"
}
variable "infisical_client_id" {
  type      = string
  sensitive = true
}

variable "infisical_client_secret" {
  type      = string
  sensitive = true
}
resource "infisical_secret" "nats_token" {
  name         = "NATS_TOKEN"
  value        = local.nats_token
  env_slug     = var.infisical_env_slug
  workspace_id = var.infisical_project_id
  folder_path  = "/dynamic"
}

resource "infisical_secret" "s3_access_key" {
  name         = "S3_ACCESS_KEY"
  value        = module.r2_s3_token.id
  env_slug     = var.infisical_env_slug
  workspace_id = var.infisical_project_id
  folder_path  = "/dynamic"
}

resource "infisical_secret" "s3_secret_key" {
  name         = "S3_SECRET_KEY"
  value        = module.r2_s3_token.secret
  env_slug     = var.infisical_env_slug
  workspace_id = var.infisical_project_id
  folder_path  = "/dynamic"
}

provider "infisical" {
  host = "https://app.infisical.com"
  auth = {
    universal = {
      client_id     = var.infisical_client_id
      client_secret = var.infisical_client_secret
    }
  }
}
