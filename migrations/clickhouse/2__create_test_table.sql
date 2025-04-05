CREATE TABLE test_table
(
    id UInt32,
    name String,
)
ENGINE = MergeTree()
ORDER BY id;



