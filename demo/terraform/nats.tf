variable "hcloud_token" {
  sensitive = true
}

provider "hcloud" {
  token = var.hcloud_token
}


variable "ssh_public_key" {
  type        = string
  description = "SSH public key for Hetzner server access"
}

variable "ssh_private_key" {
  type        = string
  description = "SSH private key for Hetzner server provisioner"
  sensitive   = true
}

resource "hcloud_ssh_key" "ssh-key" {
  name       = "ssh-key"
  public_key = var.ssh_public_key
}

resource "hcloud_server" "nats-server" {
  name         = "distcrawl-demo-nats-server"
  image        = "ubuntu-24.04"
  server_type  = "cx23"
  firewall_ids = [hcloud_firewall.nats_firewall.id]
  ssh_keys     = [hcloud_ssh_key.ssh-key.id]

  provisioner "file" {
    source      = "../docker-compose.hub.yml"
    destination = "/root/docker-compose.hub.yml"
    connection {
      type        = "ssh"
      user        = "root"
      private_key = var.ssh_private_key
      host        = self.ipv4_address
    }
  }

  provisioner "remote-exec" {
    inline = [
      "apt-get update",
      "apt-get install -y docker.io docker-compose-v2",
      "systemctl enable --now docker",
      "cd /root",
      "echo 'NATS_TOKEN=${local.nats_token}' > .env",
      "echo 'CLOUDFLARE_TUNNEL_TOKEN=${data.cloudflare_zero_trust_tunnel_cloudflared_token.cloudflare_tunnel_token.token}' >> .env",
      "docker compose -f docker-compose.hub.yml --profile cloudflare_tunnel up -d"
    ]
    connection {
      type        = "ssh"
      user        = "root"
      private_key = var.ssh_private_key
      host        = self.ipv4_address
    }
  }
}

resource "terraform_data" "nats_cleanup" {
  input = {
    ipv4_address = hcloud_server.nats-server.ipv4_address
    private_key  = var.ssh_private_key
  }

  provisioner "remote-exec" {
    when = destroy
    inline = [
      "docker compose -f /root/docker-compose.hub.yml down || true",
      "sleep 60"
    ]
    connection {
      type        = "ssh"
      user        = "root"
      private_key = self.input.private_key
      host        = self.input.ipv4_address
    }
  }
}

resource "hcloud_firewall" "nats_firewall" {
  name = "nats-firewall"
  rule {
    direction = "in"
    protocol  = "icmp"
    source_ips = [
      "0.0.0.0/0",
      "::/0"
    ]
  }

  rule {
    direction = "in"
    protocol  = "tcp"
    port      = "22"
    source_ips = [
      "0.0.0.0/0",
      "::/0"
    ]
  }
}


# tunnel so we have tls between worker and nats
resource "cloudflare_zero_trust_tunnel_cloudflared" "cloudflare_tunnel" {
  account_id = var.cloudflare_account_id
  name       = "distcrawl-demo-tunnel"
}

resource "cloudflare_zero_trust_tunnel_cloudflared_config" "cloudflare_tunnel_config" {
  account_id = var.cloudflare_account_id
  tunnel_id  = cloudflare_zero_trust_tunnel_cloudflared.cloudflare_tunnel.id
  config = {
    ingress = [{
      hostname = "nats-distcrawl-demo.landmann.ph"
      service  = "http://nats:8080"
      }
      , {
        service = "http_status:404"
      }
    ]
  }
}

data "cloudflare_zero_trust_tunnel_cloudflared_token" "cloudflare_tunnel_token" {
  account_id = var.cloudflare_account_id
  tunnel_id  = cloudflare_zero_trust_tunnel_cloudflared.cloudflare_tunnel.id
}

resource "cloudflare_dns_record" "nats_tunnel" {
  zone_id = var.cloudflare_zone_id
  type    = "CNAME"
  name    = "nats-distcrawl-demo"
  content = "${cloudflare_zero_trust_tunnel_cloudflared.cloudflare_tunnel.id}.cfargotunnel.com"
  proxied = true
  ttl     = 1
}

variable "cloudflare_api_token" {
  description = "Cloudflare API token"
  sensitive   = true
}

variable "cloudflare_account_id" {
  description = "Cloudflare account ID"
  sensitive   = true
}

variable "cloudflare_zone_id" {
  description = "Cloudflare zone ID for landmann.ph"
  sensitive   = true
}
provider "cloudflare" {
  api_token = var.cloudflare_api_token
}
