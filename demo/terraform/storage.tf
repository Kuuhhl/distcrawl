# results bucket
resource "cloudflare_r2_bucket" "distcrawl-demo-results_bucket" {
  account_id    = var.cloudflare_account_id
  name          = var.results_bucket_name
  storage_class = "Standard"
}

# we need this because cloudflare r2 does not have force_destroy :(
resource "terraform_data" "empty_r2_bucket" {
  input = {
    bucket_name           = cloudflare_r2_bucket.distcrawl-demo-results_bucket.name
    r2_s3_token_id        = module.r2_s3_token.id
    r2_s3_token_secret    = module.r2_s3_token.secret
    cloudflare_account_id = var.cloudflare_account_id
  }

  provisioner "local-exec" {
    when    = destroy
    command = "aws s3 rm s3://${self.input.bucket_name} --recursive --endpoint-url https://${self.input.cloudflare_account_id}.r2.cloudflarestorage.com"
    environment = {
      AWS_ACCESS_KEY_ID     = self.input.r2_s3_token_id
      AWS_SECRET_ACCESS_KEY = self.input.r2_s3_token_secret
      AWS_DEFAULT_REGION    = "auto"
    }
  }
}

module "r2_s3_token" {
  source     = "Cyb3r-Jak3/r2-api-token/cloudflare"
  version    = "6.0.0"
  account_id = var.cloudflare_account_id
  buckets    = [var.results_bucket_name]
}

variable "results_bucket_name" {
  description = "Name of the results bucket"
  type        = string
  default     = "distcrawl-demo-results"
}
