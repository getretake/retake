import os
import pytest
import psycopg2
import requests

from requests.auth import HTTPBasicAuth

from core.sdk.source import Source, PostgresSource
from core.sdk.transform import Transform
from core.sdk.embedding import Embedding
from core.sdk.sink import Sink
from core.sdk.target import Target


## Helpers ##


def local_elasticsearch_sink(docker_ip, docker_services):
    print(
        "\nSpinning up local ElasticSearch Docker container + fixture (this may take a minute)..."
    )

    def is_responsive(url):
        try:
            response = requests.get(
                url, auth=HTTPBasicAuth("elastic", "elastic"), verify=False
            )
            return response.status_code == 200
        except Exception as e:
            return False

    port = docker_services.port_for("elasticsearch", 9200)
    url = f"https://{docker_ip}:{port}"

    print(f"Waiting for {url} to be responsive")

    docker_services.wait_until_responsive(
        timeout=90.0, pause=1, check=lambda: is_responsive(url)
    )

    print("ElasticSearch instance is responsive!")

    return Sink.ElasticSearch(
        host="https://localhost:9200",
        user="elastic",
        password="elastic",
        ssl_assert_fingerprint=None,
    )


def ci_elasticsearch_sink():
    return Sink.ElasticSearch(
        host="https://localhost:9200",
        user="elastic",
        password="changeme",  # Default password for ElasticSearch from elastic/elastic-github-actions/elasticsearch@master
        ssl_assert_fingerprint=None,
    )


## Fixtures ##


@pytest.fixture(scope="session")
def test_table_name():
    return "test_table"


@pytest.fixture(scope="session")
def test_column_name():
    return "test_column"


@pytest.fixture(scope="session")
def test_primary_key():
    return "test_pk"


@pytest.fixture(scope="session")
def test_vector():
    return [0.1, 0.2, 0.3]


@pytest.fixture(scope="session")
def test_index_name():
    return "test_index_name"


@pytest.fixture(scope="session")
def test_field_name():
    return "test_field_name"


@pytest.fixture(scope="session")
def test_document_id():
    return "test_document_id"


@pytest.fixture
def postgres_source(
    postgresql, test_table_name, test_primary_key, test_column_name, test_document_id
):
    dsn = f"dbname={postgresql.info.dbname} user={postgresql.info.user} host={postgresql.info.host} port={postgresql.info.port}"

    # Populate DB with test data
    temp_conn = psycopg2.connect(dsn)
    with temp_conn.cursor() as cursor:
        cursor.execute(
            f"CREATE TABLE {test_table_name} ({test_primary_key} varchar PRIMARY KEY, {test_column_name} varchar);"
        )
        cursor.execute(
            f"INSERT INTO {test_table_name} VALUES ('{test_document_id}', 'fake_data1'), ('id2', 'fake_data2'), ('id3', 'fake_data3');"
        )
    temp_conn.commit()
    temp_conn.close()

    # Return Source
    source = PostgresSource(dsn=dsn)
    return source


@pytest.fixture(scope="session")
def postgres_transform(test_table_name, test_column_name, test_primary_key):
    return Transform.Postgres(
        primary_key=test_primary_key,
        relation=test_table_name,
        columns=[test_column_name],
        transform_func=lambda x: x,
    )


@pytest.fixture(scope="session")
def custom_embedding(test_vector):
    def func(documents):
        return [test_vector for document in documents]

    return Embedding.Custom(func=func)


# In CI, we run the ElasticSearch Docker container separately via a GitHub Action, so we
# create this fixture factory to only launch the container if we're running the test locally.
@pytest.fixture
def elasticsearch_sink_factory(request):
    ci_var = os.getenv("CI")

    if not ci_var:
        docker_ip = request.getfixturevalue("docker_ip")
        docker_services = request.getfixturevalue("docker_services")
        return local_elasticsearch_sink(docker_ip, docker_services)
    else:
        return ci_elasticsearch_sink()


@pytest.fixture(scope="session")
def elasticsearch_target(test_index_name, test_field_name):
    return Target.ElasticSearch(
        index_name=test_index_name,
        field_name=test_field_name,
        should_index=True,
        similarity="cosine",
    )
