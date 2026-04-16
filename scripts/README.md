# dist-scripts

CLI tools to manage and analyze crawls.

- `dist-seed` uploads metadata to object store and puts tasks in the NATS queue.
- `dist-status` shows a live dashboard of current progress.
- `dist-download` downloads the completed experiments, deduplicates and labels requests using an adblock-list.

## Usage

### Fetching Tranco

Fetch and cache the Tranco list once. That way the ranking stays the same for all runs and analysis.

```bash
uv run dist-fetch-tranco [--output-path PATH]
```

### `dist-seed`

Seed a new experiment into the queue!

```zsh
uv run dist-seed [--accept-cookies | --no-accept-cookies] \ # should we accept cookies?
    [--navigate | --no-navigate] \ # should we go deeper than the homepage?
    --depth INT \ # how many levels deep? (0 is homepage, 1 is one level deeper)
    --dwell-seconds SECS \ # applies to subpage navigation too
    --scroll-amounts PX PX ... \ # can also be negative to scroll up :) (ex. -100 200 50). use 0 to disable.
    --num-tranco N \ # specify the number of urls from the cached tranco list.
    --browser {chromium,firefox,webkit} \ # browser engine to use
    [--headless | --no-headless] \ # run in headless mode (default: no)
    [--tranco-path PATH] \ # path to the tranco cache. defaults to data/tranco_cache.
    [--nats-url URL] \ # optional
    [--results-bucket NAME] \ # also optional
    <NAME>
```

### `dist-status`

Will show a little progress indication of our experiments.

```zsh
uv run dist-status [--nats-url URL] [--results-bucket NAME]
```

### `dist-download`

Fetches the completed experiments and applies some post-processing (deduplication, combining files, labeling requests based on the provided filter-lists).

```zsh
uv run dist-download [--filter-lists URL,URL,...] # these lists will be used for labeling. i included some defaults (easyprivacy, easylist in multiple languages)
```
