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


