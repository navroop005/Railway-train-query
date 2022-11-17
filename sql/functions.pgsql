CREATE OR REPLACE PROCEDURE insert_route(
    train_id INTEGER,
    station_order INTEGER,
    station_code VARCHAR,
    arrival_time TIME(0),
    depart_time TIME(0),
    day_info INTEGER
)
LANGUAGE PLPGSQL
AS $$
    BEGIN        
        EXECUTE format('CREATE TABLE IF NOT EXISTS train_%s(station_code VARCHAR, station_order INTEGER PRIMARY KEY, arrival_time TIME(0),depart_time TIME(0), day_info INTEGER, FOREIGN KEY (station_code) REFERENCES stations(station_code))',train_id) ;
        EXECUTE format('INSERT INTO train_%s VALUES($1,$2,$3,$4,$5)',train_id) USING station_code,station_order,arrival_time,depart_time,day_info;
        EXCEPTION
            WHEN unique_violation THEN
                RAISE NOTICE 'Already exist.';
    END;
$$;

CREATE OR REPLACE PROCEDURE insert_train(
    train_id INTEGER,
    name VARCHAR
)
LANGUAGE PLPGSQL
AS $$
    BEGIN        
        INSERT INTO trains VALUES (train_id, name);
        EXCEPTION
            WHEN unique_violation THEN
    END;
$$;

CREATE OR REPLACE PROCEDURE insert_station(
    station_code VARCHAR,
    name VARCHAR
)
LANGUAGE PLPGSQL
AS $$
    BEGIN        
        INSERT INTO stations VALUES (station_code, name);
        EXCEPTION
            WHEN unique_violation THEN
    END;
$$;

CREATE OR REPLACE FUNCTION find_trains(
    src_code VARCHAR,
    dest_code VARCHAR
)
RETURNS SETOF train_rec
LANGUAGE PLPGSQL
AS $$
    DECLARE 
        train_row RECORD;
        src_row RECORD; 
        dest_row RECORD;
        temp_row RECORD;
        temp_row2 RECORD;
    BEGIN        
        CREATE TEMP TABLE src_trains(train_id INTEGER, station_order INTEGER, depart_time TIME(0), day_info INTEGER) ON COMMIT DROP;
        CREATE TEMP TABLE dest_trains(train_id INTEGER, station_order INTEGER, arrival_time TIME(0), day_info INTEGER) ON COMMIT DROP;
        
        FOR train_row IN
            SELECT * FROM trains
        LOOP
            FOR src_row in EXECUTE FORMAT( 'SELECT * FROM train_%s t WHERE t.station_code = $1',train_row.train_id) 
                USING src_code
            LOOP 
                INSERT INTO src_trains VALUES(train_row.train_id, src_row.station_order, src_row.depart_time, src_row.day_info);
            END LOOP;

            FOR dest_row in EXECUTE FORMAT( 'SELECT * FROM train_%s t WHERE t.station_code = $1',train_row.train_id) 
                USING dest_code
            LOOP 
                INSERT INTO dest_trains VALUES(train_row.train_id, dest_row.station_order, dest_row.arrival_time, dest_row.day_info);
            END LOOP;
        END LOOP;

        FOR src_row 
            IN SELECT * FROM src_trains
        LOOP
            FOR dest_row 
                IN SELECT * FROM dest_trains
            LOOP
                FOR temp_row IN EXECUTE format('SELECT t1.arrival_time, t1.day_info t1_day, t2.depart_time, t2.day_info t2_day, t1.station_code 
                    FROM train_%s t1, train_%s t2
                    WHERE t1.station_code = t2.station_code AND
                        t1.station_code <> $1 AND
                        t1.station_code <> $2 AND
                        t1.station_order >= $3 AND
                        t2.station_order <= $4', 
                        src_row.train_id, dest_row.train_id) 
                        USING src_code, dest_code, src_row.station_order , dest_row.station_order
                LOOP
                    temp_row2 := (src_row.train_id, src_row.depart_time, dest_row.train_id, dest_row.arrival_time, temp_row.station_code, temp_row.arrival_time, temp_row.depart_time, temp_row.t1_day - src_row.day_info, dest_row.day_info - temp_row.t2_day );
                    return next temp_row2 ;
                    RAISE NOTICE '% % % % % %',temp_row.t1_day , src_row.station_order ,src_row.day_info, dest_row.station_order, dest_row.day_info ,temp_row.t2_day;
                END LOOP;
            END LOOP;
        END LOOP;
    END;
$$;