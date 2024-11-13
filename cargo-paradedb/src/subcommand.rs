// Copyright (c) 2023-2024 Retake, Inc.
//
// This file is part of ParadeDB - Postgres for Search and Analytics
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use crate::benchmark::Benchmark;
use crate::tables::{benchlogs::EsLog, PathReader};
use anyhow::{bail, Result};
use cmd_lib::{run_cmd, run_fun};
use criterion::Criterion;
use futures::StreamExt;
use itertools::Itertools;
use reqwest::blocking::Client;
use serde_json::json;
use sqlx::postgres::PgPoolOptions;
use sqlx::{postgres::PgConnectOptions, Connection, PgConnection, Postgres, QueryBuilder};
use std::time::SystemTime;
use std::{fs, os::unix::process::CommandExt, str::FromStr};
use tempfile::tempdir;
use tracing::debug;

pub fn install() -> Result<()> {
    // The crate_path is available to us at compile time with env!.
    let crate_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).to_path_buf();

    // We use the --offline path so that we don't update the crates.io index
    // every time we reinstall.
    let mut command = std::process::Command::new("cargo");
    command
        .arg("install")
        .arg("--offline")
        .arg("--path")
        .arg(crate_path); // Replace with the actual path

    // Using `exec` will replace the terminate the current process and replace it
    // with the command we've defined, as opposed to running it as a subprocess.
    command.exec();
    Ok(())
}

// As a note from researching the corpus generated for elasticsearch's benchmarks...
// Elasticsearch used 1024 1GB files generated by their corpus tool. However, the
// corpus tool changed its API since then, and no longer accepts a "total_bytes"
// argument. It now accepts a "total_events" argument. While more ergonomic, it means
// that we have to work backwards if we want to generate exactly 1TB of data.
pub async fn bench_eslogs_generate(
    seed: u64,
    events: u64,
    table: String,
    url: String,
) -> Result<()> {
    // Ensure that golang and the generator tool are installed.
    if let Err(err) = run_cmd!(go version > /dev/null) {
        bail!("Golang is likely not installed... {err}")
    }

    run_cmd!(go install github.com/elastic/elastic-integration-corpus-generator-tool@latest)?;

    // We're going to use the generator configuration from the elasticsearch benchmarks.
    // Download them into temporary files so they can be passed to the generator tool.
    let config_tempdir = tempdir()?;
    let template_file = config_tempdir.path().join("template.tpl");
    let fields_file = config_tempdir.path().join("fields.yml");
    let config_file = config_tempdir.path().join("config-1.yml");

    let opensearch_repo_url =
        "https://raw.githubusercontent.com/elastic/elasticsearch-opensearch-benchmark/main";

    run_cmd!(curl -s -f -o $template_file $opensearch_repo_url/dataset/template.tpl)?;
    run_cmd!(curl -s -f -o $fields_file $opensearch_repo_url/dataset/fields.yml)?;
    run_cmd!(curl -s -f -o $config_file $opensearch_repo_url/dataset/config-1.yml)?;

    // Set up necessary executable paths to call the generator tool.
    let go_path = run_fun!(go env GOPATH)?;
    let generator_exe = format!("{go_path}/bin/elastic-integration-corpus-generator-tool");

    // Set up Postgres connection and ensure the table exists.
    debug!(DATABASE_URL = url);
    let conn_opts = &PgConnectOptions::from_str(&url)?;
    let mut conn = PgConnection::connect_with(conn_opts).await?;
    sqlx::query(&EsLog::create_table_statement(&table))
        .execute(&mut conn)
        .await?;

    // We'll skip events already created in the destination Postgres table.
    let events_already_loaded: u64 =
        sqlx::query_as::<_, (i64,)>(&format!("SELECT COUNT(id) from {}", table))
            .fetch_one(&mut conn)
            .await?
            .0 as u64;

    // The generator tool outputs to files, which we'll then read to load into Postgres.
    // We want to cap the size of the output files to a reasonable size.
    // 118891 events == 100MB of data, which is the size we'll cap each output file to.
    let events_per_file = 118891;
    let events_to_create = events - events_already_loaded;
    let files_to_create = events_to_create.div_ceil(events_per_file);
    debug!(files_to_create, events_to_create);

    // A counter for logging.
    let mut inserted_events = events_already_loaded;

    // For each generated file we intend to create, we'll run the generator tool once,
    // and then immediately load the data into Postgres.
    for file_index in 0..files_to_create {
        // Setup transaction and how many events to be generated in this transaction.
        let mut transaction = sqlx::Connection::begin(&mut conn).await?;
        let transaction_events =
            events_per_file.min(events_to_create - file_index * events_per_file);

        // We want the generated files to be deleted after inserting into Postgres,
        // so we'll make a tempdir that will be deleted when it's dropped at the
        // end of this block.
        let generated_tempdir = tempdir()?;
        let generated_dir = &generated_tempdir.path().join("generated");

        // Ensure output directory for the generated file exists.
        fs::create_dir_all(generated_dir)?;
        // The generator tool uses the DATA_DIR env var to determine output location.
        std::env::set_var("DATA_DIR", generated_dir);

        // The generator tool doesn't have many configuration options... including around
        // how it names files. We're stuck with the behavior that filenames will just be
        // timestamps (to the second). So if your `bytes` argument is so low that the
        // file can be generated under a second... it will just overwrite the previous file.
        // It only makes sense to `repeat` if you're generating lots of large files.
        let iter_seed = file_index + seed;
        run_cmd!(
            $generator_exe generate-with-template $template_file $fields_file
            --tot-events $transaction_events
            --config-file $config_file
            --template-type gotext
            --seed $iter_seed
            > /dev/null
        )?;

        // The files should have been generated, so build a glob string to match them.
        // The tool generates the files under a few nested folders, so make sure to
        // recursively glob for them.
        let output_files_glob_string = generated_dir.join("**/*.tpl").display().to_string();

        // Read event JSON, chunked to not overload Postgres.
        let log_chunks = EsLog::read_all(&output_files_glob_string)?.chunks(1000);

        // Build an INSERT statement and write to database.
        for chunk in log_chunks.into_iter() {
            QueryBuilder::<Postgres>::new(EsLog::insert_header(&table))
                .push_values(chunk, EsLog::insert_push_values)
                .build()
                .execute(&mut *transaction)
                .await?;
        }

        // Commit the transaction.
        transaction.commit().await?;

        // Log inserted events.
        inserted_events += transaction_events;
        debug!(inserted = inserted_events, "inserting json benchlog chunk");
    }
    Ok(())
}

pub async fn bench_eslogs_build_search_index(
    table: String,
    index: String,
    url: String,
) -> Result<()> {
    let drop_query =
        format!("CREATE EXTENSION IF NOT EXISTS pg_search; DROP INDEX IF EXISTS '{index}';");

    let create_query = format!(
        r#"
        CREATE INDEX {index} on {table} USING bm25 (id, message) WITH (key_field='id')
        "#
    );

    Benchmark {
        group_name: "Search Index".into(),
        function_name: "bench_eslogs_build_search_index".into(),
        // First, drop any existing index to ensure a clean environment.
        setup_query: Some(drop_query),
        query: create_query,
        database_url: url,
    }
    .run_pg_once()
    .await
}

pub async fn bench_eslogs_query_search_index(
    table: String,
    query: String,
    limit: u64,
    url: String,
) -> Result<()> {
    Benchmark {
        group_name: "Search Query".into(),
        function_name: "bench_eslogs_query_search_index".into(),
        setup_query: None,
        query: format!("SELECT * FROM {table} WHERE {table} @@@ '{query}' LIMIT {limit}"),
        database_url: url,
    }
    .run_pg()
    .await
}

pub async fn bench_eslogs_build_gin_index(table: String, index: String, url: String) -> Result<()> {
    let drop_query = format!("DROP INDEX IF EXISTS {index}");

    let create_query =
        format!("CREATE INDEX {index} ON {table} USING gin ((to_tsvector('english', message)));");

    Benchmark {
        group_name: "GIN TSQuery/TSVector Index".into(),
        function_name: "bench_eslogs_build_gin_index".into(),
        setup_query: Some(drop_query),
        query: create_query,
        database_url: url,
    }
    .run_pg_once()
    .await
}

pub async fn bench_eslogs_query_gin_index(
    table: String,
    query: String,
    limit: u64,
    url: String,
) -> Result<()> {
    Benchmark {
        group_name: "GIN TSQuery/TSVector Query".into(),
        function_name: "bench_eslogs_query_gin_index".into(),
        setup_query: None,
        query: format!("SELECT * FROM {table} WHERE to_tsvector('english', message) @@ to_tsquery('{query}') LIMIT {limit};"),
        database_url: url,
    }
    .run_pg()
    .await
}

pub async fn bench_eslogs_build_parquet_table(table: String, url: String) -> Result<()> {
    let parquet_table_name = format!("{table}_parquet");
    let drop_query = format!(
        r#"
        DROP TABLE IF EXISTS {parquet_table_name};
        CREATE TABLE {parquet_table_name} (metrics_size int4) USING parquet;
        "#
    );
    let create_query = format!(
        r#"
        INSERT INTO {parquet_table_name} (metrics_size)
        SELECT metrics_size FROM {table};
        "#
    );

    Benchmark {
        group_name: "Parquet Table".into(),
        function_name: "bench_eslogs_build_parquet_table".into(),
        // First, drop any existing table to ensure a clean environment.
        setup_query: Some(drop_query),
        query: create_query,
        database_url: url,
    }
    .run_pg_once()
    .await
}

pub async fn bench_eslogs_count_parquet_table(table: String, url: String) -> Result<()> {
    Benchmark {
        group_name: "Parquet Table".into(),
        function_name: "bench_eslogs_build_parquet_table".into(),
        setup_query: None, // First, drop any existing index to ensure a clean environment.
        query: format!("SELECT COUNT(*) FROM {table}_parquet"),
        database_url: url,
    }
    .run_pg()
    .await
}

pub async fn bench_eslogs_build_elastic_table(
    elastic_url: String,
    postgres_url: String,
    table: String,
) -> Result<()> {
    // It's expected that the elastic_url passed here already has the index name
    // as a path subcomponent. We also need to make sure it doesn't have a trailing slash.
    let build_url = format!("{}?pretty", elastic_url.trim_end_matches('/'));
    let insert_url = format!("{}/_bulk", elastic_url.trim_end_matches('/'));
    let config = crate::elastic::ELASTIC_INDEX_CONFIG.to_string();

    // Build empty Elasticsearch index.
    run_cmd!(curl -X PUT $build_url -H "Content-Type: application/json" -d $config).unwrap();

    // Setup Postgres connection.
    debug!(DATABASE_URL = postgres_url);
    let select_all_query = format!("SELECT id, message FROM {table}");
    let pg_pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&postgres_url)
        .await?;
    let cursor = sqlx::query_as::<Postgres, (i32, String)>(&select_all_query).fetch(&pg_pool);

    // We'll stream the data in chunks so that we can bulk insert into Elasticsearch.
    // Chunking at 4000, as much higher seems to cause a "too many arguments" error.
    let mut chunked_cursor = cursor.chunks(4000).enumerate();

    let start_time = SystemTime::now();

    // Insert chunks of Postgres results into Elasticsearch.
    while let Some((index, chunk)) = chunked_cursor.next().await {
        let mut index_data = vec![];

        // Flatten maintains only the Ok values.
        for (id, message) in chunk.into_iter().flatten() {
            // Each insert operation into Elasticsearch is comprised of two newline-delimited
            // JSON messages. One describing the actual operation ("index") and the next
            // containing the field data.
            index_data.push(json!({ "index": {"_id": id.to_string()} }).to_string());
            index_data.push(json!({ "message": message }).to_string());
        }

        // Each JSON message must be separated by a newline.
        run_cmd!(
            printf "%s\n" $[index_data]
            | curl -X PUT $insert_url -H "Content-Type: application/x-ndjson" --data-binary @-
            &> /dev/null
        )
        .unwrap();
        debug!(chunk = index, "wrote chunk(4000) to elasticsearch")
    }

    // Print benchmark results.
    let end_time = SystemTime::now();
    Benchmark::print_results(start_time, end_time);

    Ok(())
}

pub async fn bench_eslogs_query_elastic_table(
    elastic_url: String,
    field: String,
    term: String,
) -> Result<()> {
    let mut criterion = Criterion::default();
    let mut group = criterion.benchmark_group("Elasticsearch Index");

    // Lowered from default sample size to remove Criterion warning.
    // Must be higher than 10, or Criterion will panic.
    group.sample_size(60);
    group.bench_function("bench_eslogs_query_elastic_table", |runner| {
        // Per-sample (note that a sample can be many iterations) setup goes here.
        let search_url = format!("{}/_search", elastic_url.trim_end_matches('/'));
        let search_json = json!({
            "from": 0,
            "size": 1,
            "query": {
                "match": {
                    &field: &term
                }
            }
        });

        // Create a client instance
        let client = Client::new();
        runner.iter(|| {
            // Measured code goes here.
            let res = client
                .get(&search_url)
                .header("Content-Type", "application/json")
                .json(&search_json)
                .send()
                .expect("error sending request to elasticsearch index");

            // Parse the response text as JSON
            let response_body: serde_json::Value = res.json().unwrap();

            // Ensure correct response structure.
            response_body["hits"]["total"]["value"]
                .as_i64()
                .expect("no hits field on response, does the index exist?")
        });
    });

    group.finish();

    Ok(())
}
