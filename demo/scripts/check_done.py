import asyncio
import os
import sys

import nats

STREAM_NAME = "CRAWL"
SUBJECT_FILTER = "crawl.urls.chromium.headless.>"
TOTAL_TASKS = 3
POLL_INTERVAL = 3  # seconds


async def main():
    nats_url = os.environ.get("NATS_URL")
    nats_token = os.environ.get("NATS_TOKEN")

    if not nats_url:
        print("Error: NATS_URL is not set", file=sys.stderr)
        sys.exit(1)

    print(f"Connecting to NATS at {nats_url}...")
    connect_opts = {"token": nats_token} if nats_token else {}

    nc = await nats.connect(nats_url, **connect_opts)
    js = nc.jetstream()

    try:
        while True:
            remaining = TOTAL_TASKS
            try:
                info = await js.stream_info(STREAM_NAME, subjects_filter=SUBJECT_FILTER)
                subjects = info.state.subjects or {}
                remaining = sum(subjects.values())
            except Exception as e:
                print(f"Failed to get stream info: {e}")

            completed = TOTAL_TASKS - remaining
            print(f"Tasks: {completed}/{TOTAL_TASKS} done, {remaining} remaining")

            if remaining <= 0:
                print("All tasks complete.")
                sys.exit(0)

            await asyncio.sleep(POLL_INTERVAL)
    finally:
        await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
