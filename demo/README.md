# Live Demo Environment

This directory houses the needed infrastructure (Terraform config files, scripts) to deploy a proof of concept demo that runs a crawl on some example websites.
Terraform automatically spins up the needed infrastructure and destroys after the demo.
Results are published to [distcrawl-demo.landmann.ph](https://distcrawl-demo.landmann.ph).

Secrets are pulled dynamically from Infisical Secret Store using OIDC permissions of GitHub Actions.
Worker images are always up-to-date as we auto re-deploy after image build.
