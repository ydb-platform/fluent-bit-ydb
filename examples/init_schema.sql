CREATE TABLE `fluentbit/log` (
     `timestamp`  Timestamp NOT NULL,
     `input`   Text NOT NULL,
     `message`    Text NOT NULL,
     PRIMARY KEY (
          `timestamp`, `input`
     )
)
PARTITION BY HASH(`timestamp`, `input`)
WITH (
     STORE = COLUMN
)
