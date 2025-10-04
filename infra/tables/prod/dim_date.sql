CREATE OR REPLACE TABLE DIM_DATE (
    DATE_KEY        NUMBER(38,0)        PRIMARY KEY,
    DATE            DATE,   -- Surrogate key in YYYYMMDD format
    YEAR            INT,
    QUARTER         INT,
    MONTH           INT,
    MONTH_NAME      STRING,
    DAY             INT,
    DAY_OF_WEEK     INT,
    DAY_NAME        STRING,
    WEEK_OF_YEAR    INT
);