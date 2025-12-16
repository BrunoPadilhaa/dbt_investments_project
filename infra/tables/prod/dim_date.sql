CREATE OR REPLACE TABLE PROD.DIM_DATE (
    DATE_ID             NUMBER(38,0)    PRIMARY KEY,   -- YYYYMMDD
    DATE                DATE            NOT NULL,
    YEAR                INT             NOT NULL,
    QUARTER_NUMBER      INT             NOT NULL,
    MONTH_NUMBER        INT             NOT NULL,
    MONTH_NAME          STRING          NOT NULL,
    MONTH_ABRV          STRING          NOT NULL,
    MONTH_ABRV_YEAR     STRING          NOT NULL,
    DAY                 INT             NOT NULL,
    DAY_OF_WEEK         INT             NOT NULL,      -- ISO (Mon = 1)
    DAY_NAME            STRING          NOT NULL,
    WEEK_OF_YEAR        INT             NOT NULL
);
