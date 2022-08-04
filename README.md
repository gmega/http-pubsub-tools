http_pubsub_tools
=================

Utilities for consuming PubSub messages from Cloud Run and Cloud Function HTTP handlers.

## Install

This will install the latest version:

```{sh}
pip install https://github.com/gmega/http_pubsub_tools
```

## Running Tests

You will need [Docker](https://www.docker.com/) and [Docker Compose](https://docs.docker.com/compose/). First, at
the project's root folder, start up the supporting services with:

```{sh}
docker-compose up
```

Then, fire up pytest with:

```{sh}
PUBSUB_EMULATOR_HOST=localhost:8085 poetry run pytest
```