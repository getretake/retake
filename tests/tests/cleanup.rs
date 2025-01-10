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

mod fixtures;

use fixtures::*;
use rstest::*;
use sqlx::PgConnection;

#[rstest]
fn vacuum_full(mut conn: PgConnection) {
    SimpleProductsTable::setup().execute(&mut conn);
    "DELETE FROM paradedb.bm25_search WHERE id IN (1, 2, 3, 4, 5)".execute(&mut conn);

    "VACUUM FULL".execute(&mut conn);
}

#[rstest]
fn create_and_drop_builtin_index(mut conn: PgConnection) {
    // Test to ensure that dropping non-search indexes works correctly, as our event
    // trigger will need to skip indexes we didn't create.

    "CREATE TABLE test_table (id SERIAL PRIMARY KEY, value TEXT NOT NULL)".execute(&mut conn);

    "CREATE INDEX test_table_value_idx ON test_table(value)".execute(&mut conn);

    "DROP INDEX test_table_value_idx CASCADE".execute(&mut conn);

    let index_count = "SELECT COUNT(*) FROM pg_indexes WHERE indexname = 'test_table_value_idx'"
        .fetch_one::<(i64,)>(&mut conn)
        .0;

    assert_eq!(
        index_count, 0,
        "Index should no longer exist after dropping with CASCADE"
    );

    "DROP TABLE IF EXISTS test_table CASCADE".execute(&mut conn);
}

#[rstest]
fn segment_count_matches_available_parallelism(mut conn: PgConnection) {
    // CREATE INDEX should create as many segments as there are available CPUs
    r#"
        SET maintenance_work_mem = '1GB';
        DROP TABLE IF EXISTS test_table;
        CREATE TABLE test_table (id SERIAL PRIMARY KEY, value TEXT NOT NULL);
        INSERT INTO test_table (value) SELECT md5(random()::text) FROM generate_series(1, 100000);

        CREATE INDEX idxtest_table ON public.test_table
        USING bm25 (id, value)
        WITH (
            key_field = 'id',
            text_fields = '{
                "value": {}
            }'
        );
    "#
    .execute(&mut conn);

    let expected_segments: usize = std::thread::available_parallelism().unwrap().into();
    let nsegments = "SELECT COUNT(*) FROM paradedb.index_info('idxtest_table');"
        .fetch_one::<(i64,)>(&mut conn)
        .0 as usize;
    assert_eq!(nsegments, expected_segments);

    // nplusone merge policy should merge single documents down to 1 segment
    for _ in 0..10 {
        "INSERT INTO test_table (value) SELECT md5(random()::text)".execute(&mut conn);
    }

    let nsegments = "SELECT COUNT(*) FROM paradedb.index_info('idxtest_table');"
        .fetch_one::<(i64,)>(&mut conn)
        .0 as usize;
    assert_eq!(nsegments, expected_segments + 1);
}

#[rstest]
fn segment_count_exceeds_target(mut conn: PgConnection) {
    r#"
        SET maintenance_work_mem = '64MB';
        DROP TABLE IF EXISTS test_table;
        CREATE TABLE test_table (id SERIAL PRIMARY KEY, value TEXT NOT NULL);
        INSERT INTO test_table (value) SELECT md5(random()::text) FROM generate_series(1, 100000);

        CREATE INDEX idxtest_table ON public.test_table
        USING bm25 (id, value)
        WITH (
            key_field = 'id',
            text_fields = '{
                "value": {}
            }'
        );
    "#
    .execute(&mut conn);

    let nsegments_prev = "SELECT COUNT(*) FROM paradedb.index_info('idxtest_table');"
        .fetch_one::<(i64,)>(&mut conn)
        .0 as usize;
    assert!(nsegments_prev >= 15);

    "INSERT INTO test_table (value) SELECT md5(random()::text)".execute(&mut conn);

    let nsegments = "SELECT COUNT(*) FROM paradedb.index_info('idxtest_table');"
        .fetch_one::<(i64,)>(&mut conn)
        .0 as usize;
    assert_eq!(nsegments_prev + 1, nsegments);
}

#[rstest]
fn vacuum_restores_segment_count(mut conn: PgConnection) {
    r#"
        SET maintenance_work_mem = '1GB';
        DROP TABLE IF EXISTS test_table;
        CREATE TABLE test_table (id SERIAL PRIMARY KEY, value TEXT NOT NULL);
        INSERT INTO test_table (value) SELECT md5(random()::text) FROM generate_series(1, 100000);

        CREATE INDEX idxtest_table ON public.test_table
        USING bm25 (id, value)
        WITH (
            key_field = 'id',
            text_fields = '{
                "value": {}
            }'
        );
    "#
    .execute(&mut conn);

    r#"
        INSERT INTO test_table (value) SELECT md5(random()::text);
        INSERT INTO test_table (value) SELECT md5(random()::text);
        INSERT INTO test_table (value) SELECT md5(random()::text);
        INSERT INTO test_table (value) SELECT md5(random()::text);
        INSERT INTO test_table (value) SELECT md5(random()::text);
        INSERT INTO test_table (value) SELECT md5(random()::text);
    "#.execute(&mut conn);

    "VACUUM test_table".execute(&mut conn);

    let expected_segments: usize = std::thread::available_parallelism().unwrap().into();
    let nsegments = "SELECT COUNT(*) FROM paradedb.index_info('idxtest_table');"
        .fetch_one::<(i64,)>(&mut conn)
        .0 as usize;
    assert_eq!(nsegments, expected_segments + 1);
}

#[rstest]
fn bulk_insert_merge_behavior(mut conn: PgConnection) {
    r#"
        SET maintenance_work_mem = '1GB';
        DROP TABLE IF EXISTS test_table;
        CREATE TABLE test_table (id SERIAL PRIMARY KEY, value TEXT NOT NULL);

        CREATE INDEX idxtest_table ON public.test_table
        USING bm25 (id, value)
        WITH (
            key_field = 'id',
            text_fields = '{
                "value": {}
            }'
        );
    "#
    .execute(&mut conn);

    "INSERT INTO test_table (value) SELECT md5(random()::text) FROM generate_series(1, 100000)".execute(&mut conn);

    let expected_segments: usize = std::thread::available_parallelism().unwrap().into();
    let nsegments = "SELECT COUNT(*) FROM paradedb.index_info('idxtest_table');"
        .fetch_one::<(i64,)>(&mut conn)
        .0 as usize;
    assert_eq!(nsegments, expected_segments);

    "INSERT INTO test_table (value) SELECT md5(random()::text)".execute(&mut conn);

    let nsegments = "SELECT COUNT(*) FROM paradedb.index_info('idxtest_table');"
        .fetch_one::<(i64,)>(&mut conn)
        .0 as usize;
    assert_eq!(nsegments, expected_segments + 1);
}

