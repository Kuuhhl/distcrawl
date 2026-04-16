# dist-common

Shared package used by both the worker and the scripts.

`models.py` has the three pydantic models:
* `ExperimentParams` (the crawl config with the parameters we used when seeding)
* `ExperimentMetadata` (what gets stored in the object store when you seed)
* `CrawlTask` (what gets published to NATS for each URL to give the worker all info it needs).

`types.py` has the TypedDicts that specify the format for saving requests, responses, and cookie consent events.

`config.py` has `BaseCrawlSettings`, the pydantic-settings base class that both the worker and scripts extend to read NATS connection details, stream/consumer names, timeouts, etc. from env vars. See `.env.example` for all available settings.
