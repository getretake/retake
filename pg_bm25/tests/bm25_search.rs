mod fixtures;

use core::panic;

use fixtures::*;
use pretty_assertions::assert_eq;
use rstest::*;
use sqlx::PgConnection;

#[rstest]
async fn basic_search_query(mut conn: PgConnection) -> Result<(), sqlx::Error> {
    SimpleProductsTable::setup().execute(&mut conn);

    let columns: SimpleProductsTableVec =
        "SELECT * FROM bm25_search.search('description:keyboard OR category:electronics')"
            .fetch_collect(&mut conn);

    assert_eq!(
        columns.description,
        concat!(
            "Plastic Keyboard,Ergonomic metal keyboard,Innovative wireless earbuds,",
            "Fast charging power bank,Bluetooth-enabled speaker"
        )
        .split(',')
        .collect::<Vec<_>>()
    );

    assert_eq!(
        columns.category,
        "Electronics,Electronics,Electronics,Electronics,Electronics"
            .split(',')
            .collect::<Vec<_>>()
    );

    Ok(())
}

#[rstest]
async fn basic_search_ids(mut conn: PgConnection) {
    SimpleProductsTable::setup().execute(&mut conn);

    let columns: SimpleProductsTableVec =
        "SELECT * FROM bm25_search.search('description:keyboard OR category:electronics')"
            .fetch_collect(&mut conn);
    assert_eq!(columns.id, vec![2, 1, 12, 22, 32]);

    let columns: SimpleProductsTableVec =
        "SELECT * FROM bm25_search.search('description:keyboard')".fetch_collect(&mut conn);
    assert_eq!(columns.id, vec![2, 1]);
}

#[rstest]
fn with_bm25_scoring(mut conn: PgConnection) {
    SimpleProductsTable::setup().execute(&mut conn);

    let rows: Vec<(i32, f32)> = "SELECT id, paradedb.rank_bm25(id) FROM bm25_search.search('category:electronics OR description:keyboard')"
        .fetch(&mut conn);

    let ids: Vec<_> = rows.iter().map(|r| r.0).collect();
    let expected = [2, 1, 12, 22, 32];
    assert_eq!(ids, expected);

    let ranks: Vec<_> = rows.iter().map(|r| r.1).collect();
    let expected = [5.3764954, 4.931014, 2.1096356, 2.1096356, 2.1096356];
    assert_eq!(ranks, expected);
}

#[rstest]
fn json_search(mut conn: PgConnection) {
    SimpleProductsTable::setup().execute(&mut conn);

    let columns: SimpleProductsTableVec =
        "SELECT * FROM bm25_search.search('metadata.color:white')".fetch_collect(&mut conn);
    assert_eq!(columns.id, vec![4, 15, 25]);
}

#[rstest]
fn real_time_search(mut conn: PgConnection) {
    SimpleProductsTable::setup().execute(&mut conn);

    "INSERT INTO paradedb.bm25_search (description, rating, category, in_stock, metadata) VALUES ('New keyboard', 5, 'Electronics', true, '{}')"
        .execute(&mut conn);
    "DELETE FROM paradedb.bm25_search WHERE id = 1".execute(&mut conn);
    "UPDATE paradedb.bm25_search SET description = 'PVC Keyboard' WHERE id = 2".execute(&mut conn);

    let columns: SimpleProductsTableVec = "SELECT * FROM bm25_search.search('description:keyboard OR category:electronics') ORDER BY id"
        .fetch_collect(&mut conn);
    assert_eq!(columns.id, vec![2, 12, 22, 32, 42]);
}

#[rstest]
fn sequential_scan_syntax(mut conn: PgConnection) {
    SimpleProductsTable::setup().execute(&mut conn);

    let columns: SimpleProductsTableVec = "SELECT * FROM paradedb.bm25_search
        WHERE paradedb.search_tantivy(
            paradedb.bm25_search.*,
            jsonb_build_object(
                'index_name', 'bm25_search_bm25_index',
                'table_name', 'bm25_test_table',
                'schema_name', 'paradedb',
                'key_field', 'id',
                'query', paradedb.parse('category:electronics')::text::jsonb
            )
        ) ORDER BY id"
        .fetch_collect(&mut conn);

    assert_eq!(columns.id, vec![1, 2, 12, 22, 32]);
}

#[rstest]
fn quoted_table_name(mut conn: PgConnection) {
    r#"CREATE TABLE "Activity" (key SERIAL, name TEXT, age INTEGER);
    INSERT INTO "Activity" (name, age) VALUES ('Alice', 29);
    INSERT INTO "Activity" (name, age) VALUES ('Bob', 34);
    INSERT INTO "Activity" (name, age) VALUES ('Charlie', 45);
    INSERT INTO "Activity" (name, age) VALUES ('Diana', 27);
    INSERT INTO "Activity" (name, age) VALUES ('Fiona', 38);
    INSERT INTO "Activity" (name, age) VALUES ('George', 41);
    INSERT INTO "Activity" (name, age) VALUES ('Hannah', 22);
    INSERT INTO "Activity" (name, age) VALUES ('Ivan', 30);
    INSERT INTO "Activity" (name, age) VALUES ('Julia', 25);
    CALL paradedb.create_bm25(
    	index_name => 'activity',
    	table_name => 'Activity',
    	key_field => 'key',
    	text_fields => '{"name": {}}'
    )"#
    .execute(&mut conn);
    let row: (i32, String, i32) =
        "SELECT * FROM activity.search('name:alice')".fetch_one(&mut conn);

    assert_eq!(row, (1, "Alice".into(), 29));
}

#[rstest]
fn text_arrays(mut conn: PgConnection) {
    r#"CREATE TABLE example_table (
        id SERIAL PRIMARY KEY,
        text_array TEXT[],
        varchar_array VARCHAR[]
    );
    INSERT INTO example_table (text_array, varchar_array) VALUES 
    ('{"text1", "text2", "text3"}', '{"vtext1", "vtext2"}'),
    ('{"another", "array", "of", "texts"}', '{"vtext3", "vtext4", "vtext5"}'),
    ('{"single element"}', '{"single varchar element"}');
    CALL paradedb.create_bm25(
    	index_name => 'example_table',
    	table_name => 'example_table',
    	key_field => 'id',
    	text_fields => '{text_array: {}, varchar_array: {}}'
    )"#
    .execute(&mut conn);
    let row: (i32,) =
        r#"SELECT * FROM example_table.search('text_array:text1')"#.fetch_one(&mut conn);

    assert_eq!(row, (1,));

    let row: (i32,) =
        r#"SELECT * FROM example_table.search('text_array:"single element"')"#.fetch_one(&mut conn);

    assert_eq!(row, (3,));

    let rows: Vec<(i32,)> =
        r#"SELECT * FROM example_table.search('varchar_array:varchar OR text_array:array')"#
            .fetch(&mut conn);

    assert_eq!(rows[0], (3,));
    assert_eq!(rows[1], (2,));
}

#[rstest]
fn uuid(mut conn: PgConnection) {
    r#"
    CREATE TABLE uuid_table (
        id SERIAL PRIMARY KEY,
        random_uuid UUID,
        some_text text
    );

    INSERT INTO uuid_table (random_uuid, some_text) VALUES ('f159c89e-2162-48cd-85e3-e42b71d2ecd0', 'some text');
    INSERT INTO uuid_table (random_uuid, some_text) VALUES ('38bf27a0-1aa8-42cd-9cb0-993025e0b8d0', 'some text');
    INSERT INTO uuid_table (random_uuid, some_text) VALUES ('b5faacc0-9eba-441a-81f8-820b46a3b57e', 'some text');
    INSERT INTO uuid_table (random_uuid, some_text) VALUES ('eb833eb6-c598-4042-b84a-0045828fceea', 'some text');
    INSERT INTO uuid_table (random_uuid, some_text) VALUES ('ea1181a0-5d3e-4f5f-a6ab-b1354ffc91ad', 'some text');
    INSERT INTO uuid_table (random_uuid, some_text) VALUES ('28b6374a-67d3-41c8-93af-490712f9923e', 'some text');
    INSERT INTO uuid_table (random_uuid, some_text) VALUES ('f6e85626-298e-4112-9abb-3856f8aa046a', 'some text');
    INSERT INTO uuid_table (random_uuid, some_text) VALUES ('88345d21-7b89-4fd6-87e4-83a4f68dbc3c', 'some text');
    INSERT INTO uuid_table (random_uuid, some_text) VALUES ('40bc9216-66d0-4ae8-87ee-ddb02e3e1b33', 'some text');
    INSERT INTO uuid_table (random_uuid, some_text) VALUES ('02f9789d-4963-47d5-a189-d9c114f5cba4', 'some text');
    
    -- Ensure that indexing works with UUID present on table.
    CALL paradedb.create_bm25(
    	index_name => 'uuid_table',
        table_name => 'uuid_table',
        key_field => 'id',
        text_fields => '{"some_text": {}}'
    );
    
    CALL paradedb.drop_bm25('uuid_table');"#
        .execute(&mut conn);

    match r#"
    CALL paradedb.create_bm25(
        index_name => 'uuid_table',
        table_name => 'uuid_table',
        key_field => 'id',
        text_fields => '{"some_text": {}, "random_uuid": {}}'
    )"#
    .execute_result(&mut conn)
    {
        Err(err) => assert!(err.to_string().contains("cannot be indexed")),
        _ => panic!("uuid fields in bm25 index should not be supported"),
    };
}

#[rstest]
fn hybrid(mut conn: PgConnection) {
    SimpleProductsTable::setup().execute(&mut conn);
    r#"
    CREATE EXTENSION vector;
    ALTER TABLE paradedb.bm25_search ADD COLUMN embedding vector(3);

    UPDATE paradedb.bm25_search m
    SET embedding = ('[' ||
    ((m.id + 1) % 10 + 1)::integer || ',' ||
    ((m.id + 2) % 10 + 1)::integer || ',' ||
    ((m.id + 3) % 10 + 1)::integer || ']')::vector;

    CREATE INDEX on paradedb.bm25_search
    USING hnsw (embedding vector_l2_ops)"#
        .execute(&mut conn);

    // Test with query object.
    let columns: SimpleProductsTableVec = r#"
    SELECT m.*, s.rank_hybrid
    FROM paradedb.bm25_search m
    LEFT JOIN (
        SELECT * FROM bm25_search.rank_hybrid(
            bm25_query => paradedb.parse('description:keyboard OR category:electronics'),
            similarity_query => '''[1,2,3]'' <-> embedding',
            bm25_weight => 0.9,
            similarity_weight => 0.1
        )
    ) s ON m.id = s.id
    LIMIT 5"#
        .fetch_collect(&mut conn);

    assert_eq!(columns.id, vec![2, 1, 29, 39, 9]);

    // Test with string query.
    let columns: SimpleProductsTableVec = r#"
    SELECT m.*, s.rank_hybrid
    FROM paradedb.bm25_search m
    LEFT JOIN (
        SELECT * FROM bm25_search.rank_hybrid(
            bm25_query => 'description:keyboard OR category:electronics',
            similarity_query => '''[1,2,3]'' <-> embedding',
            bm25_weight => 0.9,
            similarity_weight => 0.1
        )
    ) s ON m.id = s.id
    LIMIT 5"#
        .fetch_collect(&mut conn);

    assert_eq!(columns.id, vec![2, 1, 29, 39, 9]);
}

#[rstest]
fn multi_tree(mut conn: PgConnection) {
    SimpleProductsTable::setup().execute(&mut conn);
    let columns: SimpleProductsTableVec = r#"
    SELECT * FROM bm25_search.search(
	    query => paradedb.boolean(
		    should => ARRAY[
			    paradedb.parse('description:shoes'),
			    paradedb.phrase_prefix(field => 'description', phrases => ARRAY['book']),
			    paradedb.term(field => 'description', value => 'speaker'),
			    paradedb.fuzzy_term(field => 'description', value => 'wolo')
		    ]
	    )
	);
    "#
    .fetch_collect(&mut conn);
    assert_eq!(columns.id, vec![32, 5, 3, 4, 7, 34, 37, 10, 33, 39, 41]);
}

#[rstest]
fn highlight(mut conn: PgConnection) {
    SimpleProductsTable::setup().execute(&mut conn);
    let row: (String,) = "
        SELECT paradedb.highlight(id, 'description')
        FROM bm25_search.search('description:shoes')"
        .fetch_one(&mut conn);
    assert_eq!(row.0, "Generic <b>shoes</b>");

    let row: (String,) = "
        SELECT paradedb.highlight(id, 'description', prefix => '<h1>', postfix => '</h1>')
        FROM bm25_search.search('description:shoes')"
        .fetch_one(&mut conn);
    assert_eq!(row.0, "Generic <h1>shoes</h1>")
}

#[rstest]
fn alias(mut conn: PgConnection) {
    SimpleProductsTable::setup().execute(&mut conn);

    let rows = "
        SELECT id, paradedb.highlight(id, field => 'description') FROM bm25_search.search('description:shoes')
        UNION
        SELECT id, paradedb.highlight(id, field => 'description')
        FROM bm25_search.search('description:speaker')
        ORDER BY id"
        .fetch_result::<()>(&mut conn);

    match rows {
        Ok(_) => panic!("an alias should be required for multiple search calls"),
        Err(err) => assert!(err
            .to_string()
            .contains("could not store search state in manager: AliasRequired")),
    }

    let rows: Vec<(i32, String)> = "
        SELECT id, paradedb.highlight(id, field => 'description') FROM bm25_search.search('description:shoes')
        UNION
        SELECT id, paradedb.highlight(id, field => 'description', alias => 'speaker')
        FROM bm25_search.search('description:speaker', alias => 'speaker')
        ORDER BY id"
        .fetch(&mut conn);

    assert_eq!(rows[0].0, 3);
    assert_eq!(rows[1].0, 4);
    assert_eq!(rows[2].0, 5);
    assert_eq!(rows[3].0, 32);
    assert_eq!(rows[0].1, "Sleek running <b>shoes</b>");
    assert_eq!(rows[1].1, "White jogging <b>shoes</b>");
    assert_eq!(rows[2].1, "Generic <b>shoes</b>");
    assert_eq!(rows[3].1, "Bluetooth-enabled <b>speaker</b>");
}
