# Telemetry Data Structures

This is the format we use to store telemetry data in our object store / data lake.
We partition by experiment and data type and flush our results to `.parquet`-files regularly.

## Requests

This is the structure of a `.parquet`-file containing captured requests:

| column | type | description |
| :--- | :--- | :--- |
| worker_id | string | join key to worker_metadata |
| experiment_id | string | id of the crawl experiment |
| **request_id** | string | unique id for the request-response pair |
| crawl_session_id| string | unique id for the specific page crawl (url and its subpages) |
| url | string | target url of the request |
| crawled_url | string | the top-level url being crawled |
| current_page_url | string | the specific deep link where the request was triggered |
| frame_url | string | url of the frame that initiated the request |
| resource_type | string | e.g. document, script, image |
| method | string | http method (get, post, etc.) |
| headers | string | json-encoded request headers |
| timestamp | string | unix timestamp in milliseconds format|
| crawl_depth | int | depth level from the seed url |

## Responses

This is how responses are saved:

| column | type | description |
| :--- | :--- | :--- |
| request_id | string | join key to requests |
| status | int | http status code |
| url | string | final response url (after redirects) |
| headers | string | json-encoded response headers |
| cookies | string | json-encoded response cookies |
| timestamp | string | unix timestamp in ms|

Notice how we can pair up requests and responses later in SQL by joining on `request_id`.

### Experiment Metadata

Each experiment is documented with its parameters in a `metadata.json` file inside its experiment directory:

| field | type | description |
| :--- | :--- | :--- |
| id | string | the unique identifier for this experiment |
| timestamp | string | iso8601 creation timestamp |
| total_urls | int | number of seed urls in this experiment |
| params | object | dict of experiment configuration parameters |

#### Example

```json
{
  "id": "beispiel_fdbf4205",
  "timestamp": "2026-04-03T18:41:37Z",
  "total_urls": 100,
  "params": {
    "auto_accept_cookies": true,
    "navigate_subpages": true,
    "max_depth": 2,
    "dwell_time": 5,
    "scroll_amounts": [0, 500, 1000],
    "browser_type": "chromium",
    "headless": true
  }
}
```

## Site Metadata

Extracted once per crawl session after page load, stored under `data_type=site_metadata`:

| column | type | description |
| :--- | :--- | :--- |
| crawl_session_id | string | join key to requests |
| timestamp | string | unix timestamp of extraction |
| description | string | content of the page's `<meta name="description">` tag (max 500 chars) |

### Worker Metadata

This is flushed once per worker startup under `/experiment=system`:

| column | type | description |
| :--- | :--- | :--- |
| **worker_id** | string | primary key for the worker |
| country_code | string | two-letter country code (e.g. "nl") |
| is_residential | boolean | true if using a residential ip |
| timestamp | float | unix timestamp of worker startup |

## Cookie Warning Consents

Stored under `data_type=cookie_warning_consents` when cookie acceptance is enabled:

| column | type | description |
| :--- | :--- | :--- |
| experiment_id | string | id of the crawl experiment |
| crawl_session_id | string | unique id for the specific page crawl |
| crawled_url | string | the top-level url being crawled |
| url | string | url of the page where the consent was clicked |
| timestamp | string | unix timestamp in milliseconds format |
| crawl_depth | int | depth level from the seed url |

## Storage Layout

```text
object-store-bucket/
├── experiment=<id>/
│   ├── metadata.json
│   ├── data_type=requests/
│   │   └── <timestamp>_<hash>.parquet
│   │   └── ...
│   ├── data_type=responses/
│   │   └── <timestamp>_<hash>.parquet
│   │   └── ...
│   ├── data_type=site_metadata/
│   │   └── <timestamp>_<hash>.parquet
│   │   └── ...
│   └── data_type=cookie_warning_consents/
│       └── <timestamp>_<hash>.parquet
│       └── ...
└── experiment=system/
    ├── metadata.json
    └── data_type=worker_metadata/
        └── <timestamp>_<hash>.parquet
        └── ...
```
