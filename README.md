<p align="center">
  <img src="assets/retake.svg" alt="Retake" width="125px"></a>
</p>

<h1 align="center">
    <b>Retake</b>
</h1>

<p align="center">
    <b>Real-Time Neural Search for Developers</b> <br />
</p>

Retake is real-time keyword + semantic neural search infrastructure for developers, built to stay in sync with fast-changing databases. Retake wraps around any Postgres database and provides simple search SDKs that snap into any Python or Typescript application. You don't need to worry about managing separate vector stores and text search engines, uploading and embedding documents, or reindexing data. Just write search queries and let Retake handle the rest.

To get started, simply start the Retake engine

```
docker compose up
```

By default, this will start the Retake engine at `http://localhost:8000` with API key `retake-test-key`.

## Usage

### Python

Install the SDK

```
pip install retakesearch
```

The core API is just two functions

```
from retakesearch import Client, Search, Database, Table

client = Client(api_key="retake-test-key", url="http://localhost:8000")

database = Database(
    host-"***",
    user="***",
    password="***",
    port=5432
)

table = Table(
    name="table_name",
    primary_key="primary_key_column",
    columns: ["column1"] # These are the columns you wish to search
    neural_columns=["column1"] # These are the columns you wish to enable neural search over
)

# Index your table
# This only needs to be done once
client.index(database, table)

# Search your table
query = Search().neuralQuery("my_query", ["column1])
response = client.search("table_name", query)

print(response)
```

## Key Features

- :arrows_counterclockwise: **Always in Sync**: Built with Kafka and OpenSearch, Retake connects to a Postgres database, indexes, and automatically creates embeddings for tables and columns specified by the developer. As data changes or new data arrives in Postgres, Retake ensures that the indexed data and its embeddings are kept in sync.
- :rocket: **Low-Code SDK**: Retake provides intuitive search SDKs that drop into any Python application (other languages coming soon). The core API is just two functions.
- :zap: **Open/ElasticSearch DSL Compatible**: Retake’s query interface is built on top of the the high-level OpenSearch Python client, enabling developers to query with the full expressiveness of the OpenSearch DSL (domain-specific language).
- :globe_with_meridians: **Deployable Anywhere**: Retake is deployable anywhere, from a laptop to a distributed cloud system.

## How Retake Works 

A detailed overview of Retake's architecture can be found in our [documentation](https://docs.getretake.com/architecture).

## Contributing

For more information on how to contribute, please see our [Contributing Guide](CONTRIBUTING.md).

## License

Retake is [Elastic License 2.0 licensed](LICENSE).
