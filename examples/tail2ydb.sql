CREATE TABLE `fluentbit/log` (
     `timestamp`  Timestamp NOT NULL,
     `input`      Text NOT NULL,
     `datahash`   Uint64 NOT NULL,
     `message`    Text NOT NULL,
     PRIMARY KEY (
          `timestamp`, `input`, `datahash`
     )
) PARTITION BY HASH(`timestamp`, `input`)
WITH (
     STORE = COLUMN
);
