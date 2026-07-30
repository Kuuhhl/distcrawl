locals {
  nats_token = random_password.password.result
}

resource "random_password" "password" {
  length           = 256
  special          = true
  override_special = "_-+@*/.~:"
}
resource "random_password" "short_hash" {
  length  = 36
  special = false
  upper   = false
}

terraform {
  cloud {
    workspaces {
      name = "distcrawl-demo"
    }
  }

  required_providers {
    cloudflare = {
      source  = "cloudflare/cloudflare"
      version = "~> 5.13"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3"
    }
    infisical = {
      source  = "infisical/infisical"
      version = "~> 0.15.0"
    }
    hcloud = {
      source  = "hetznercloud/hcloud"
      version = "~> 1.45"
    }
    saladcloud = {
      source  = "squat/saladcloud"
      version = "~> 0.3.0"
    }
  }
}
