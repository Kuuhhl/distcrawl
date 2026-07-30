import asyncio
import json
import os
import uuid
from datetime import datetime

import nats
import obstore as obs
from nats.js.api import RetentionPolicy
from nats.js.errors import BadRequestError


async def main() -> int:
    nats_url = os.environ.get("NATS_URL")
    nats_token = os.environ.get("NATS_TOKEN")
    s3_bucket = os.environ.get("RESULTS_BUCKET_NAME")
    cloudflare_account_id = os.environ.get("TF_VAR_cloudflare_account_id")
    s3_access_key = os.environ.get("S3_ACCESS_KEY")
    s3_secret_key = os.environ.get("S3_SECRET_KEY")

    if not all(
        [nats_url, s3_bucket, cloudflare_account_id, s3_access_key, s3_secret_key]
    ):
        print("Some values not set, skipping seed.")
        return 1

    print(f"Connecting to NATS at {nats_url}...")

    connect_opts = {"token": nats_token} if nats_token else {}

    nc = await nats.connect(nats_url, **connect_opts)
    try:
        js = nc.jetstream()

        stream_name = "CRAWL"
        subject_prefix = "crawl.urls"
        try:
            await js.add_stream(
                name=stream_name,
                subjects=[f"{subject_prefix}.>"],
                retention=RetentionPolicy.WORK_QUEUE,
            )
        except BadRequestError as e:
            if "already in use" not in str(e):
                raise
            print(f"Stream {stream_name} already exists, continuing.")

        exp_id = f"demo_{str(uuid.uuid4())[:8]}"
        subject = f"{subject_prefix}.chromium.headless.{exp_id}"

        urls = [
            "https://wikipedia.com",
            "https://duckduckgo.com",
            "https://landmann.ph",
        ]

        params = {
            "auto_accept_cookies": True,
            "navigate_subpages": False,
            "max_depth": 0,
            "dwell_time": 15,
            "scroll_amounts": [500, 1000, -500, -1000],
            "browser_type": "chromium",
            "headless": True,
        }

        config = {
            "experiment_id": exp_id,
            "timestamp": datetime.now().isoformat(),
            "total_urls": len(urls),
            "params": params,
        }

        print(f"Starting seed for experiment: {exp_id}")
        print(f"Publishing to subject: {subject}")

        s3_endpoint = f"https://{cloudflare_account_id}.r2.cloudflarestorage.com"
        try:
            store = obs.store.S3Store(
                bucket=s3_bucket,
                endpoint=s3_endpoint,
                access_key_id=s3_access_key,
                secret_access_key=s3_secret_key,
            )
            config_key = f"experiment={exp_id}/metadata.json"
            await obs.put_async(
                store,
                config_key,
                json.dumps(config, indent=2, default=str).encode("utf-8"),
            )
            print(f"Config uploaded to object store: {config_key}")
        except Exception as e:
            print(f"Failed to upload config to object store: {e}")
            return 1

        for url in urls:
            task_data = params.copy()
            task_data.update({"url": url, "experiment_id": exp_id})

            payload = json.dumps(task_data).encode("utf-8")

            ack = await js.publish(subject, payload)
            print(f"Published task for {url} | ACK Sequence: {ack.seq}")
    finally:
        await nc.close()

    print("Finished seeding tasks.")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
