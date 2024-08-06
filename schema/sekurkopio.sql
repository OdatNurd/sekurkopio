--------------------------------------------------------------------------------


DROP TABLE IF EXISTS BackupList;
CREATE TABLE BackupList (
    id INTEGER UNIQUE PRIMARY KEY AUTOINCREMENT,

    dbName TEXT,
    backupName TEXT
);


--------------------------------------------------------------------------------
