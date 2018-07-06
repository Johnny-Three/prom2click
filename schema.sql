# note:REPLACE {shard} AND {REPLICA} AND run ON EACH SERVER


// LOCAL test TABLE ON ubuntu ..
CREATE TABLE metrics.samples (
  ip            STRING   DEFAULT 'default',
  app           STRING   DEFAULT 'x',
  name          STRING   DEFAULT 'x',
  job           STRING   DEFAULT 'x',
  namespace     STRING   DEFAULT 'x',
  shard         STRING   DEFAULT 'x',
  keyspace      STRING   DEFAULT 'x',
  component     STRING   DEFAULT 'x',
  containername STRING   DEFAULT 'x',
  val           FLOAT64,
  ts            DATETIME,
  date          DATE     DEFAULT toDate(0),
  tags ARRAY (String),
  updated       DATETIME DEFAULT now()
)
  ENGINE = MergeTree PARTITION BY toMonday (date
) ORDER BY (date, NAME, ts
) SETTINGS index_granularity = 8192


// LOCAL TABLE ON Centos ..
CREATE TABLE metrics.samples (
  ip            STRING   DEFAULT 'default',
  app           STRING   DEFAULT 'x',
  name          STRING   DEFAULT 'x',
  job           STRING   DEFAULT 'x',
  namespace     STRING   DEFAULT 'x',
  shard         STRING   DEFAULT 'x',
  keyspace      STRING   DEFAULT 'x',
  component     STRING   DEFAULT 'x',
  containername STRING   DEFAULT 'x',
  val           FLOAT64,
  ts            DATETIME,
  date          DATE     DEFAULT toDate(0),
  tags ARRAY (String),
  updated       DATETIME DEFAULT now()
)ENGINE = MergeTree PARTITION BY toMonday (date) ORDER BY (date, NAME, ts) SETTINGS index_granularity = 8192;



CREATE TABLE metrics.samples (
  ip            STRING   DEFAULT 'default',
  app           STRING   DEFAULT 'x',
  name          STRING   DEFAULT 'x',
  job           STRING   DEFAULT 'x',
  namespace     STRING   DEFAULT 'x',
  shard         STRING   DEFAULT 'x',
  keyspace      STRING   DEFAULT 'x',
  component     STRING   DEFAULT 'x',
  containername STRING   DEFAULT 'x',
  val           FLOAT64,
  ts            DATETIME,
  date          DATE     DEFAULT toDate(0),
  tags ARRAY (String),
  updated       DATETIME DEFAULT now()
)ENGINE = ReplicatedGraphiteMergeTree ('/clickhouse/tables/{shard}/metrics.samples','{replica}', partition by toMonday(date) order by  (date, name, ts) settings index_granularity=8192);

===线上操作步骤===

DROP DATABASE IF EXISTS metrics  ON CLUSTER ads_app_clickhouse_cluster;

create database metrics on cluster ads_app_clickhouse_cluster;
 
CREATE TABLE IF NOT EXISTS metrics.samples ON CLUSTER ads_app_clickhouse_cluster(
  ip            String   DEFAULT 'x',
  app           String   DEFAULT 'x',
  name          String   DEFAULT 'x',
  job           String   DEFAULT 'x',
  namespace     String   DEFAULT 'x',
  shard         String   DEFAULT 'x',
  keyspace      String   DEFAULT 'x',
  component     String   DEFAULT 'x',
  containername String   DEFAULT 'x',
  val           Float64,
  ts            DateTime,
  date          Date     DEFAULT toDate(0),
  tags          Array(String),
  updated       DateTime DEFAULT now()
)ENGINE = ReplicatedGraphiteMergeTree('/clickhouse/tables/{shard}/metrics.samples','{replica}') PARTITION BY toMonday(date) ORDER BY (date, name, ts) SETTINGS index_granularity = 8192;
