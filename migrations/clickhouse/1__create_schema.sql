CREATE TABLE clicks
(
    id UInt64,
    advertiser_id UInt32,
    campaign_id UInt32,
    timestamp DateTime,
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (timestamp, advertiser_id, campaign_id, id);

CREATE TABLE impressions
(
    id UInt64,
    advertiser_id UInt32,
    campaign_id UInt32,
    timestamp DateTime,
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (timestamp, advertiser_id, campaign_id, id);

CREATE TABLE advertiser
(
    id UInt32,
    name String,
    updated_at DateTime,
    created_at DateTime
)
ENGINE = MergeTree()
ORDER BY id;

CREATE TABLE campaign
(
    id UInt32,
    name String,
    bid DECIMAL(10,2),
    budget DECIMAL(10,2),
    start_date DATE32,
    end_date DATE32,
    advertiser_id UInt32,
    updated_at DateTime,
    created_at DateTime
)
ENGINE = MergeTree()
ORDER BY id;

