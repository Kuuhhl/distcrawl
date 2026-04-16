import time
import asyncio
import requests


def wait_for_nats(url: str = "http://localhost:18222/healthz", timeout: int = 60):
    """Wait for nats to be ready."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            resp = requests.get(url, timeout=5)
            if resp.status_code == 200:
                return True
        except Exception:
            pass
        time.sleep(2)
    return False


async def wait_for_completion(timeout: int = 120, interval: int = 5):
    """Wait for all nats consumers to finish processing."""
    elapsed = 0
    while elapsed < timeout:
        await asyncio.sleep(interval)
        elapsed += interval

        stats = get_nats_stats()
        if not stats.get("consumers"):
            continue

        for c in stats["consumers"]:
            pending = c.get("num_pending", 0)
            ack_pending = c.get("num_ack_pending", 0)
            ack_seq = c.get("delivered", {}).get("consumer_seq", 0)

            if pending == 0 and ack_pending == 0 and ack_seq > 0:
                return True
    return False


def get_nats_stats(
    url: str = "http://localhost:18222/jsz?accounts=true&streams=true&consumers=true",
):
    """Fetch jetstream stats from nats monitoring api."""
    try:
        resp = requests.get(url, timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            consumers_list = []

            # NATS monitoring API structure can vary by version
            # Try to find consumers in account_details
            if "account_details" in data:
                for acc in data.get("account_details", []):
                    for s in acc.get("stream_detail", []):
                        consumers_list.extend(s.get("consumer_detail", []))

            # Fallback for different NATS versions
            if not consumers_list and "stream_details" in data:
                for s in data.get("stream_details", []):
                    consumers_list.extend(s.get("consumer_details", []))

            # Another fallback: direct consumers array (only if it is a list)
            if not consumers_list and "consumers" in data and isinstance(data["consumers"], list):
                consumers_list = data["consumers"]

            return {"consumers": consumers_list}
    except Exception:
        pass
    return {}
