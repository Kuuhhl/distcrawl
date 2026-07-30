locals {
  name_hash_suffix = random_password.short_hash.result
}

resource "saladcloud_container_group" "distcrawl-workers" {
  autostart_policy = true
  container = {
    environment_variables = {
      NATS_URL                           = "wss://nats-distcrawl-demo.landmann.ph"
      NATS_TOKEN                         = local.nats_token
      S3_ENDPOINT_URL                    = "https://${var.cloudflare_account_id}.r2.cloudflarestorage.com"
      S3_ACCESS_KEY                      = module.r2_s3_token.id
      S3_SECRET_KEY                      = module.r2_s3_token.secret
      STORAGE_TYPE                       = "s3"
      NATS_STREAM                        = "CRAWL"
      RESULTS_BUCKET_NAME                = "${var.results_bucket_name}"
      NATS_SUBJECT_PREFIX                = "crawl.urls"
      NATS_ACK_WAIT_SECONDS              = 45
      ONLY_ALLOW_RESIDENTIAL_CONNECTIONS = "FALSE"
      NUM_CRAWLERS                       = 1 # only single context for demo
      HEADLESS                           = "TRUE"
      BROWSER_TYPE                       = "chromium"
      GOTO_TIMEOUT_MS                    = 20000
      SCROLL_DELAY_SECONDS               = 0.2
      NATS_CONNECT_TIMEOUT               = 5.0
      NATS_RECONNECT_WAIT                = 2.0
      PERSISTENCE_BATCH_SIZE             = 10000
      FLUSH_THRESHOLD                    = 1
      FETCH_TIMEOUT                      = 10.0
      HEARTBEAT_SECONDS                  = 5
      IDLE_SLEEP                         = 0.1
      QUEUE_RETRY_DELAY_SECONDS          = 5.0
      WATCHDOG_TIMEOUT_SECONDS           = 600.0
      MAX_RETRIES                        = 3
      LOGGING_LEVEL                      = "INFO"
      SEED_PUBLISH_BATCH_SIZE            = 3
    }
    image = "ghcr.io/kuuhhl/distcrawl/worker-chromium-headless:latest"
    resources = {
      cpu    = 2
      memory = 4096
      # shm_size = 2048 # up to 50% of container memory, has to be implemented in provider still.
    }
  }
  display_name      = "distcrawl-demo-workers"
  name              = "distcrawl-demo-workers-${local.name_hash_suffix}"
  organization_name = "philipp"
  project_name      = "default"
  replicas          = 1
  restart_policy    = "always"
}

variable "saladcloud_api_key" {
  type        = string
  description = "SaladCloud API key"
  sensitive   = true
}

provider "saladcloud" {
  api_key = var.saladcloud_api_key
}

# Patch shm_size because the provider does not expose it yet.
# this hack can be removed once the provider exposes shm_size.
resource "terraform_data" "patch_shm_size" {
  triggers_replace = [
    saladcloud_container_group.distcrawl-workers.id,
  ]

  provisioner "local-exec" {
    command = <<SCRIPT
URL="https://api.salad.com/api/public/organizations/${saladcloud_container_group.distcrawl-workers.organization_name}/projects/${saladcloud_container_group.distcrawl-workers.project_name}/containers/${saladcloud_container_group.distcrawl-workers.name}"
BODY='{"container":{"resources":{"cpu":2,"gpu_classes":[],"memory":4096,"shm_size":2048}}}'
MAX_RETRIES=300

i=0
while [ $i -lt $MAX_RETRIES ]; do
  i=$((i + 1))
  echo "Attempt $i: PATCH $URL"

  HTTP_CODE=$(curl -s -o /tmp/salad_patch_resp.json -w "%%{http_code}" -X PATCH "$URL" \
    -H "Content-Type: application/merge-patch+json" \
    -H "Salad-Api-Key: ${var.saladcloud_api_key}" \
    -d "$BODY")

  echo "HTTP $HTTP_CODE"

  if [ "$HTTP_CODE" = "200" ]; then
    echo "Success!"
    cat /tmp/salad_patch_resp.json
    exit 0
  fi

  if [ -f /tmp/salad_patch_resp.json ]; then
    cat /tmp/salad_patch_resp.json >&2
  fi

  echo "Retrying in 1s..."
  sleep 1
done

echo "Failed to patch shm_size after $MAX_RETRIES attempts." >&2
exit 1
SCRIPT
  }

  depends_on = [saladcloud_container_group.distcrawl-workers]
}
