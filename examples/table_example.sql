CREATE TABLE logging_example (
     event_time Timestamp NOT NULL,
     metadata   String,
     message    Json,
     PRIMARY KEY (
                  event_time
         ))
PARTITION BY HASH(event_time, metadata)
WITH (
    STORE = COLUMN
    )