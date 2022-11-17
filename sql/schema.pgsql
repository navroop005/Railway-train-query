CREATE TABLE IF NOT EXISTS trains (
    train_id INTEGER PRIMARY KEY,
    name VARCHAR
);

CREATE TABLE IF NOT EXISTS stations (
    station_code VARCHAR PRIMARY KEY,
    name VARCHAR
); 