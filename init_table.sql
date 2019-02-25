CREATE TABLE IF NOT EXISTS workers(
            ID INT NOT NULL,
            Password VARCHAR(99) NOT NULL,
            Superior INT,
            Data VARCHAR(100),
            PRIMARY KEY (ID),
            FOREIGN KEY (Superior) REFERENCES workers(ID) ON DELETE CASCADE);

CREATE ROLE app WITH LOGIN PASSWORD 'projektdb';

GRANT SELECT, UPDATE, INSERT, DELETE ON TABLE workers TO app;