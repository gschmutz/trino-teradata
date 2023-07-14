# Trino Teradata Connector

## Teradata Query Log

```sql
BEGIN QUERY LOGGING WITH SQL ON dbc;
```

```sql
FLUSH QUERY LOGGING WITH SQL;
```

```sql
END QUERY LOGGING ON dbc;
```

```sql
SELECT  SessionID, 
		ProcID,
        UserName,
        QueryText,
        StartTime
FROM    DBC.QryLogV
//WHERE   SessionID = 1184
ORDER BY    StartTime DESC;
```

## Data Structure for Tests

**Table for data structure tests**

```sql
DROP TABLE HR.datatypes;

CREATE SET TABLE HR.datatypes
     (
      id INTEGER,
      byteint_val BYTEINT,
      smallint_val SMALLINT,
      integer_val INTEGER,
      bigint_val BIGINT,
      real_val REAL,
      float_val FLOAT,
      double_val DOUBLE PRECISION,
      numeric_val NUMERIC,
      decimal_val DECIMAL,
      char_val CHAR,
      varchar_val VARCHAR(10),
      clob_val CLOB,
      byte_val BYTE,
      varbyte_val VARBYTE(1),
      blob_val BLOB,
      date_val DATE,
      time_val TIME,
      timewtz_val TIME WITH TIME ZONE,     
      timestamp_val TIMESTAMP,
      timestampwtz_val TIMESTAMP WITH TIME ZONE,
      intervaly_val INTERVAL YEAR,
      intervalym_val INTERVAL YEAR TO MONTH,
      period_val PERIOD(DATE)
      )
UNIQUE PRIMARY INDEX ( id );


INSERT INTO datatypes 
(id, byteint_val, smallint_val, integer_val, bigint_val, real_val, float_val
, double_val, numeric_val, decimal_val, char_val, varchar_val, clob_val
, byte_val, varbyte_val, blob_val, date_val, time_val, timewtz_val 
, timestamp_val, timestampwtz_val, intervaly_val, intervalym_val, period_val)
VALUES (1, 1, 1, 1, 1, 1.0, 1.0
, 1.0, 1.0, 1.0, 'A', 'ABC', EMPTY_CLOB()
, to_byte(1), to_byte(1), EMPTY_BLOB(), CURRENT_DATE, CURRENT_TIME, CURRENT_TIME 
, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 1, null, null);
```

**Table for pushdown query testss**

```sql
CREATE SET TABLE val.customer ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      cust_id INTEGER,
      income INTEGER,
      age SMALLINT,
      years_with_bank SMALLINT,
      nbr_children SMALLINT,
      gender CHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,
      marital_status CHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,
      name_prefix CHAR(4) CHARACTER SET LATIN NOT CASESPECIFIC,
      first_name CHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
      last_name CHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
      street_nbr SMALLINT,
      street_name CHAR(30) CHARACTER SET LATIN NOT CASESPECIFIC,
      postal_code CHAR(5) CHARACTER SET LATIN NOT CASESPECIFIC,
      city_name CHAR(20) CHARACTER SET LATIN NOT CASESPECIFIC,
      state_code CHAR(2) CHARACTER SET LATIN NOT CASESPECIFIC)
UNIQUE PRIMARY INDEX ( cust_id );

CREATE SET TABLE val.accounts ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
     (
      acct_nbr CHAR(16) CHARACTER SET LATIN NOT CASESPECIFIC NOT NULL,
      cust_id INTEGER,
      acct_type CHAR(2) CHARACTER SET LATIN NOT CASESPECIFIC,
      account_active CHAR(1) CHARACTER SET LATIN NOT CASESPECIFIC,
      acct_start_date DATE FORMAT 'YYYY-MM-DD',
      acct_end_date DATE FORMAT 'YYYY-MM-DD',
      starting_balance DECIMAL(9,2),
      ending_balance DECIMAL(9,2))
UNIQUE PRIMARY INDEX ( acct_nbr );
```

## Various Data Types Queries


```sql
select * from teradata.hr.datatypes 
```

returns

```
Name            |Value                  |
----------------+-----------------------+
id              |1                      |
byteint_val     |1                      |
smallint_val    |1                      |
integer_val     |1                      |
bigint_val      |1                      |
real_val        |1.0                    |
float_val       |1.0                    |
double_val      |1.0                    |
numeric_val     |1                      |
decimal_val     |1                      |
char_val        |A                      |
varchar_val     |ABC                    |
date_val        |2023-07-14             |
time_val        |10:20:31               |
timewtz_val     |10:20:31               |
timestamp_val   |2023-07-14 10:20:31.950|
timestampwtz_val|2023-07-14 10:20:31.950|
```

## Simple Where Queries

### SELECT on id (PK)

**Trino**

```sql
select id, varchar_val, smallint_val from teradata.hr.datatypes dt where dt.id  = 1
```

**Teradata**

```sql
SELECT "id", "smallint_val", "varchar_val" FROM "HR"."datatypes" WHERE "id" = ?
```

**Trino Execution Plan**

```
Trino version: 414
Fragment 0 [SOURCE]
    Output layout: [id, varchar_val, smallint_val]
    Output partitioning: SINGLE []
    Output[columnNames = [id, varchar_val, smallint_val]]
    │   Layout: [id:integer, varchar_val:varchar(10), smallint_val:smallint]
    │   Estimates: {rows: ? (?), cpu: 0, memory: 0B, network: 0B}
    └─ TableScan[table = teradata:hr.datatypes HR.datatypes constraint on [id] columns=[id:integer:INTEGER, smallint_val:smallint:SMALLINT, varchar_val:varchar(10):VARCHAR]]
           Layout: [id:integer, smallint_val:smallint, varchar_val:varchar(10)]
           Estimates: {rows: ? (?), cpu: ?, memory: 0B, network: 0B}
           varchar_val := varchar_val:varchar(10):VARCHAR
           smallint_val := smallint_val:smallint:SMALLINT
           id := id:integer:INTEGER
```

### SELECT on byteint_val

**Trino**

```sql
select id, varchar_val, byteint_val from teradata.hr.datatypes dt where byteint_val  = 1
```

**Teradata**

```sql
SELECT "id", "byteint_val", "varchar_val" FROM "HR"."datatypes" WHERE "byteint_val" = ?
```

**Trino Execution Plan**

```
Trino version: 414
Fragment 0 [SOURCE]
    Output layout: [id, varchar_val, byteint_val]
    Output partitioning: SINGLE []
    Output[columnNames = [id, varchar_val, byteint_val]]
    │   Layout: [id:integer, varchar_val:varchar(10), byteint_val:tinyint]
    │   Estimates: {rows: ? (?), cpu: 0, memory: 0B, network: 0B}
    └─ TableScan[table = teradata:hr.datatypes HR.datatypes constraint on [byteint_val] columns=[id:integer:INTEGER, byteint_val:tinyint:BYTEINT, varchar_val:varchar(10):VARCHAR]]
           Layout: [id:integer, byteint_val:tinyint, varchar_val:varchar(10)]
           Estimates: {rows: ? (?), cpu: ?, memory: 0B, network: 0B}
           varchar_val := varchar_val:varchar(10):VARCHAR
           byteint_val := byteint_val:tinyint:BYTEINT
           id := id:integer:INTEGER
```

### SELECT on smallint_val

**Trino**

```sql
select id, varchar_val, smallint_val from teradata.hr.datatypes dt where smallint_val  = 1
```

**Teradata**

```sql
SELECT "id", "smallint_val", "varchar_val" FROM "HR"."datatypes" WHERE "smallint_val" = ?
```

**Trino Execution Plan**

```
Trino version: 414
Fragment 0 [SOURCE]
    Output layout: [id, varchar_val, smallint_val]
    Output partitioning: SINGLE []
    Output[columnNames = [id, varchar_val, smallint_val]]
    │   Layout: [id:integer, varchar_val:varchar(10), smallint_val:smallint]
    │   Estimates: {rows: ? (?), cpu: 0, memory: 0B, network: 0B}
    └─ TableScan[table = teradata:hr.datatypes HR.datatypes constraint on [smallint_val] columns=[id:integer:INTEGER, smallint_val:smallint:SMALLINT, varchar_val:varchar(10):VARCHAR]]
           Layout: [id:integer, smallint_val:smallint, varchar_val:varchar(10)]
           Estimates: {rows: ? (?), cpu: ?, memory: 0B, network: 0B}
           varchar_val := varchar_val:varchar(10):VARCHAR
           smallint_val := smallint_val:smallint:SMALLINT
           id := id:integer:INTEGER
```

### SELECT on integer_val

**Trino**

```sql
select id, varchar_val, integer_val from teradata.hr.datatypes dt where integer_val  = 1
```

**Teradata**

```sql
SELECT "id", "integer_val", "varchar_val" FROM "HR"."datatypes" WHERE "integer_val" = ?
```

**Trino Execution Plan**

```
Trino version: 414
Fragment 0 [SOURCE]
    Output layout: [id, varchar_val, integer_val]
    Output partitioning: SINGLE []
    Output[columnNames = [id, varchar_val, integer_val]]
    │   Layout: [id:integer, varchar_val:varchar(10), integer_val:integer]
    │   Estimates: {rows: ? (?), cpu: 0, memory: 0B, network: 0B}
    └─ TableScan[table = teradata:hr.datatypes HR.datatypes constraint on [integer_val] columns=[id:integer:INTEGER, integer_val:integer:INTEGER, varchar_val:varchar(10):VARCHAR]]
           Layout: [id:integer, integer_val:integer, varchar_val:varchar(10)]
           Estimates: {rows: ? (?), cpu: ?, memory: 0B, network: 0B}
           varchar_val := varchar_val:varchar(10):VARCHAR
           integer_val := integer_val:integer:INTEGER
           id := id:integer:INTEGER
```


### SELECT on bigint_val

**Trino**

```sql
select id, varchar_val, bigint_val from teradata.hr.datatypes dt where bigint_val  = 1
```

**Teradata**

```sql
SELECT "id", "bigint_val", "varchar_val" FROM "HR"."datatypes" WHERE "bigint_val" = ?
```

**Trino Execution Plan**

```
Trino version: 414
Fragment 0 [SOURCE]
    Output layout: [id, varchar_val, bigint_val]
    Output partitioning: SINGLE []
    Output[columnNames = [id, varchar_val, bigint_val]]
    │   Layout: [id:integer, varchar_val:varchar(10), bigint_val:bigint]
    │   Estimates: {rows: ? (?), cpu: 0, memory: 0B, network: 0B}
    └─ TableScan[table = teradata:hr.datatypes HR.datatypes constraint on [bigint_val] columns=[id:integer:INTEGER, bigint_val:bigint:BIGINT, varchar_val:varchar(10):VARCHAR]]
           Layout: [id:integer, bigint_val:bigint, varchar_val:varchar(10)]
           Estimates: {rows: ? (?), cpu: ?, memory: 0B, network: 0B}
           varchar_val := varchar_val:varchar(10):VARCHAR
           id := id:integer:INTEGER
           bigint_val := bigint_val:bigint:BIGINT
```

### SELECT on real_val

**Trino**

```sql
select id, varchar_val, real_val from teradata.hr.datatypes dt where real_val  = 1
```

**Teradata**

```sql
SELECT "id", "real_val", "varchar_val" FROM "HR"."datatypes" WHERE "real_val" = ?
```

**Trino Execution Plan**

```
Trino version: 414
Fragment 0 [SOURCE]
    Output layout: [id, varchar_val, real_val]
    Output partitioning: SINGLE []
    Output[columnNames = [id, varchar_val, real_val]]
    │   Layout: [id:integer, varchar_val:varchar(10), real_val:double]
    │   Estimates: {rows: ? (?), cpu: 0, memory: 0B, network: 0B}
    └─ TableScan[table = teradata:hr.datatypes HR.datatypes constraint on [real_val] columns=[id:integer:INTEGER, real_val:double:FLOAT, varchar_val:varchar(10):VARCHAR]]
           Layout: [id:integer, real_val:double, varchar_val:varchar(10)]
           Estimates: {rows: ? (?), cpu: ?, memory: 0B, network: 0B}
           real_val := real_val:double:FLOAT
           varchar_val := varchar_val:varchar(10):VARCHAR
           id := id:integer:INTEGER
```

### SELECT on float_val

**Trino**

```sql
select id, varchar_val, float_val from teradata.hr.datatypes dt where float_val  = 1
```

**Teradata**

```sql
SELECT "id", "float_val", "varchar_val" FROM "HR"."datatypes" WHERE "float_val" = ?
```

**Trino Execution Plan**

```
Trino version: 414
Fragment 0 [SOURCE]
    Output layout: [id, varchar_val, float_val]
    Output partitioning: SINGLE []
    Output[columnNames = [id, varchar_val, float_val]]
    │   Layout: [id:integer, varchar_val:varchar(10), float_val:double]
    │   Estimates: {rows: ? (?), cpu: 0, memory: 0B, network: 0B}
    └─ TableScan[table = teradata:hr.datatypes HR.datatypes constraint on [float_val] columns=[id:integer:INTEGER, float_val:double:FLOAT, varchar_val:varchar(10):VARCHAR]]
           Layout: [id:integer, float_val:double, varchar_val:varchar(10)]
           Estimates: {rows: ? (?), cpu: ?, memory: 0B, network: 0B}
           float_val := float_val:double:FLOAT
           varchar_val := varchar_val:varchar(10):VARCHAR
           id := id:integer:INTEGER
```

### SELECT on double_val

**Trino**

```sql
select id, varchar_val, double_val from teradata.hr.datatypes dt where double_val  = 1
```

**Teradata**

```sql
select id, varchar_val, double_val from teradata.hr.datatypes dt where double_val  = 1
```

**Trino Execution Plan**

```
Trino version: 414
Fragment 0 [SOURCE]
    Output layout: [id, varchar_val, double_val]
    Output partitioning: SINGLE []
    Output[columnNames = [id, varchar_val, double_val]]
    │   Layout: [id:integer, varchar_val:varchar(10), double_val:double]
    │   Estimates: {rows: ? (?), cpu: 0, memory: 0B, network: 0B}
    └─ TableScan[table = teradata:hr.datatypes HR.datatypes constraint on [double_val] columns=[id:integer:INTEGER, double_val:double:FLOAT, varchar_val:varchar(10):VARCHAR]]
           Layout: [id:integer, double_val:double, varchar_val:varchar(10)]
           Estimates: {rows: ? (?), cpu: ?, memory: 0B, network: 0B}
           varchar_val := varchar_val:varchar(10):VARCHAR
           double_val := double_val:double:FLOAT
           id := id:integer:INTEGER
```

### SELECT on decimal_val

**Trino**

```sql
select id, varchar_val, decimal_val from teradata.hr.datatypes dt where decimal_val  = 1
```

**Teradata**

```sql
SELECT "id", "bigint_val", "varchar_val" FROM "HR"."datatypes" WHERE "decimal_val" = ?
```

**Trino Execution Plan**

```
Trino version: 414
Fragment 0 [SOURCE]
    Output layout: [id, varchar_val, bigint_val]
    Output partitioning: SINGLE []
    Output[columnNames = [id, varchar_val, bigint_val]]
    │   Layout: [id:integer, varchar_val:varchar(10), bigint_val:bigint]
    │   Estimates: {rows: ? (?), cpu: 0, memory: 0B, network: 0B}
    └─ TableScan[table = teradata:hr.datatypes HR.datatypes constraint on [decimal_val] columns=[id:integer:INTEGER, bigint_val:bigint:BIGINT, varchar_val:varchar(10):VARCHAR]]
           Layout: [id:integer, bigint_val:bigint, varchar_val:varchar(10)]
           Estimates: {rows: ? (?), cpu: ?, memory: 0B, network: 0B}
           varchar_val := varchar_val:varchar(10):VARCHAR
           id := id:integer:INTEGER
           bigint_val := bigint_val:bigint:BIGINT
```

### SELECT on numeric_val

**Trino**

```sql
select id, varchar_val, numeric_val from teradata.hr.datatypes dt where numeric_val  = 1
```

**Teradata**

```sql
SELECT "id", "numeric_val", "varchar_val" FROM "HR"."datatypes" WHERE "numeric_val" = ?
```

**Trino Execution Plan**

```
Trino version: 414
Fragment 0 [SOURCE]
    Output layout: [id, varchar_val, numeric_val]
    Output partitioning: SINGLE []
    Output[columnNames = [id, varchar_val, numeric_val]]
    │   Layout: [id:integer, varchar_val:varchar(10), numeric_val:decimal(5,0)]
    │   Estimates: {rows: ? (?), cpu: 0, memory: 0B, network: 0B}
    └─ TableScan[table = teradata:hr.datatypes HR.datatypes constraint on [numeric_val] columns=[id:integer:INTEGER, numeric_val:decimal(5,0):DECIMAL, varchar_val:varchar(10):VARCHAR]]
           Layout: [id:integer, numeric_val:decimal(5,0), varchar_val:varchar(10)]
           Estimates: {rows: ? (?), cpu: ?, memory: 0B, network: 0B}
           varchar_val := varchar_val:varchar(10):VARCHAR
           id := id:integer:INTEGER
           numeric_val := numeric_val:decimal(5,0):DECIMAL
```


### SELECT on char_val

**Trino**

```sql
select id, varchar_val, char_val from teradata.hr.datatypes dt where char_val  = 'A'
```

**Teradata**

```sql
SELECT "id", "char_val", "varchar_val" FROM "HR"."datatypes" WHERE "char_val" = ?```

**Trino Execution Plan**

```
Trino version: 414
Fragment 0 [SOURCE]
    Output layout: [id, varchar_val, char_val]
    Output partitioning: SINGLE []
    Output[columnNames = [id, varchar_val, char_val]]
    │   Layout: [id:integer, varchar_val:varchar(10), char_val:char(1)]
    │   Estimates: {rows: ? (?), cpu: 0, memory: 0B, network: 0B}
    └─ TableScan[table = teradata:hr.datatypes HR.datatypes constraint on [char_val] columns=[id:integer:INTEGER, char_val:char(1):CHAR, varchar_val:varchar(10):VARCHAR]]
           Layout: [id:integer, char_val:char(1), varchar_val:varchar(10)]
           Estimates: {rows: ? (?), cpu: ?, memory: 0B, network: 0B}
           varchar_val := varchar_val:varchar(10):VARCHAR
           id := id:integer:INTEGER
           char_val := char_val:char(1):CHAR
```

### SELECT on varchar_val

**Trino**

```sql
select id, varchar_val from teradata.hr.datatypes dt where varchar_val  = 'A'
```

**Teradata**

```sql
SELECT "id", "varchar_val" FROM "HR"."datatypes" WHERE "varchar_val" = ?
```

**Trino Execution Plan**

```
Trino version: 414
Fragment 0 [SOURCE]
    Output layout: [id, varchar_val]
    Output partitioning: SINGLE []
    Output[columnNames = [id, varchar_val]]
    │   Layout: [id:integer, varchar_val:varchar(10)]
    │   Estimates: {rows: ? (?), cpu: 0, memory: 0B, network: 0B}
    └─ TableScan[table = teradata:hr.datatypes HR.datatypes constraint on [varchar_val] columns=[id:integer:INTEGER, varchar_val:varchar(10):VARCHAR]]
           Layout: [id:integer, varchar_val:varchar(10)]
           Estimates: {rows: ? (?), cpu: ?, memory: 0B, network: 0B}
           varchar_val := varchar_val:varchar(10):VARCHAR
           id := id:integer:INTEGER
```

### SELECT on date_val

**Trino**

```sql
select id, date_val from teradata.hr.datatypes dt where date_val  = DATE '2023-07-13'
```

**Teradata**

```sql
SELECT "id", "date_val" FROM "HR"."datatypes" WHERE "date_val" = ?
```

**Trino Execution Plan**

```
Trino version: 414
Fragment 0 [SOURCE]
    Output layout: [id, date_val]
    Output partitioning: SINGLE []
    Output[columnNames = [id, date_val]]
    │   Layout: [id:integer, date_val:date]
    │   Estimates: {rows: ? (?), cpu: 0, memory: 0B, network: 0B}
    └─ TableScan[table = teradata:hr.datatypes HR.datatypes constraint on [date_val] columns=[id:integer:INTEGER, date_val:date:DATE]]
           Layout: [id:integer, date_val:date]
           Estimates: {rows: ? (?), cpu: ?, memory: 0B, network: 0B}
           date_val := date_val:date:DATE
           id := id:integer:INTEGER
```

### SELECT on time_val

**Trino**

```sql
select id, varchar_val , time_val  from teradata.hr.datatypes dt where time_val  = TIME '09:19:58'
```

**Teradata**

```sql
SELECT "id", "varchar_val", "time_val" FROM "HR"."datatypes" WHERE "time_val" = ?
```

**Trino Execution Plan**

```
Trino version: 414
Fragment 0 [SOURCE]
    Output layout: [id, varchar_val, time_val]
    Output partitioning: SINGLE []
    Output[columnNames = [id, varchar_val, time_val]]
    │   Layout: [id:integer, varchar_val:varchar(10), time_val:time(3)]
    │   Estimates: {rows: ? (?), cpu: 0, memory: 0B, network: 0B}
    └─ TableScan[table = teradata:hr.datatypes HR.datatypes constraint on [time_val] columns=[id:integer:INTEGER, varchar_val:varchar(10):VARCHAR, time_val:time(3):TIME]]
           Layout: [id:integer, varchar_val:varchar(10), time_val:time(3)]
           Estimates: {rows: ? (?), cpu: ?, memory: 0B, network: 0B}
           varchar_val := varchar_val:varchar(10):VARCHAR
           id := id:integer:INTEGER
           time_val := time_val:time(3):TIME
```

### SELECT on timestamp_val

**Trino**

```sql
select id, varchar_val , timestamp_val  from teradata.hr.datatypes dt where timestamp_val  = TIMESTAMP '2023-07-13 09:19:58.350'
```

**Teradata**

```sql
SELECT "id", "varchar_val", "timestamp_val" FROM "HR"."datatypes" WHERE "timestamp_val" = ?
```

**Trino Execution Plan**

```
Trino version: 414
Fragment 0 [SOURCE]
    Output layout: [id, varchar_val, timestamp_val]
    Output partitioning: SINGLE []
    Output[columnNames = [id, varchar_val, timestamp_val]]
    │   Layout: [id:integer, varchar_val:varchar(10), timestamp_val:timestamp(6)]
    │   Estimates: {rows: ? (?), cpu: 0, memory: 0B, network: 0B}
    └─ TableScan[table = teradata:hr.datatypes HR.datatypes constraint on [timestamp_val] columns=[id:integer:INTEGER, varchar_val:varchar(10):VARCHAR, timestamp_val:timestamp(6):TIMESTAMP]]
           Layout: [id:integer, varchar_val:varchar(10), timestamp_val:timestamp(6)]
           Estimates: {rows: ? (?), cpu: ?, memory: 0B, network: 0B}
           varchar_val := varchar_val:varchar(10):VARCHAR
           timestamp_val := timestamp_val:timestamp(6):TIMESTAMP
           id := id:integer:INTEGER
```

## Order By Queries

Trino

```sql
SELECT cust_id, first_name, last_name, street_nbr, income
FROM teradata.val.customer c
order by c.income desc
```

```
Trino version: 414
Fragment 0 [SINGLE]
    Output layout: [cust_id, first_name, last_name, street_nbr, income]
    Output partitioning: SINGLE []
    Output[columnNames = [cust_id, first_name, last_name, street_nbr, income]]
    │   Layout: [cust_id:integer, first_name:char(30), last_name:char(30), street_nbr:smallint, income:integer]
    │   Estimates: {rows: ? (?), cpu: 0, memory: 0B, network: 0B}
    └─ RemoteMerge[sourceFragmentIds = [1]]
           Layout: [cust_id:integer, income:integer, first_name:char(30), last_name:char(30), street_nbr:smallint]

Fragment 1 [ROUND_ROBIN]
    Output layout: [cust_id, income, first_name, last_name, street_nbr]
    Output partitioning: SINGLE []
    LocalMerge[orderBy = [income DESC NULLS LAST]]
    │   Layout: [cust_id:integer, income:integer, first_name:char(30), last_name:char(30), street_nbr:smallint]
    │   Estimates: {rows: ? (?), cpu: 0, memory: 0B, network: 0B}
    └─ PartialSort[orderBy = [income DESC NULLS LAST]]
       │   Layout: [cust_id:integer, income:integer, first_name:char(30), last_name:char(30), street_nbr:smallint]
       └─ RemoteSource[sourceFragmentIds = [2]]
              Layout: [cust_id:integer, income:integer, first_name:char(30), last_name:char(30), street_nbr:smallint]

Fragment 2 [SOURCE]
    Output layout: [cust_id, income, first_name, last_name, street_nbr]
    Output partitioning: ROUND_ROBIN []
    TableScan[table = teradata:val.customer val.customer columns=[cust_id:integer:INTEGER, income:integer:INTEGER, first_name:char(30):CHAR, last_name:char(30):CHAR, street_nbr:smallint:SMALLINT]]
        Layout: [cust_id:integer, income:integer, first_name:char(30), last_name:char(30), street_nbr:smallint]
        Estimates: {rows: ? (?), cpu: ?, memory: 0B, network: 0B}
        income := income:integer:INTEGER
        street_nbr := street_nbr:smallint:SMALLINT
        last_name := last_name:char(30):CHAR
        cust_id := cust_id:integer:INTEGER
        first_name := first_name:char(30):CHAR
```

Teradata

```
SELECT "cust_id", "income", "first_name", "last_name", "street_nbr" FROM "val"."customer"
```

## Limit Queries

### Trino

```
SELECT cust_id, first_name, last_name, street_nbr, income 
FROM teradata.val.customer  
LIMIT 10;
```

```
Trino version: 414
Fragment 0 [SOURCE]
    Output layout: [cust_id, first_name, last_name, street_nbr, income]
    Output partitioning: SINGLE []
    Output[columnNames = [cust_id, first_name, last_name, street_nbr, income]]
    │   Layout: [cust_id:integer, first_name:char(30), last_name:char(30), street_nbr:smallint, income:integer]
    │   Estimates: {rows: 10 (1.20kB), cpu: 0, memory: 0B, network: 0B}
    └─ TableScan[table = teradata:val.customer val.customer limit=10 columns=[cust_id:integer:INTEGER, income:integer:INTEGER, first_name:char(30):CHAR, last_name:char(30):CHAR, street_nbr:smallint:SMALLINT]]
           Layout: [cust_id:integer, income:integer, first_name:char(30), last_name:char(30), street_nbr:smallint]
           Estimates: {rows: 10 (1.20kB), cpu: 1.20k, memory: 0B, network: 0B}
           income := income:integer:INTEGER
           street_nbr := street_nbr:smallint:SMALLINT
           last_name := last_name:char(30):CHAR
           cust_id := cust_id:integer:INTEGER
           first_name := first_name:char(30):CHAR
```


### Teradata

```sql
SELECT TOP 10 * FROM (SELECT "cust_id", "income", "first_name", "last_name", "street_nbr" FROM "val"."customer") o
```

## Top-N Queries

```
SELECT cust_id, first_name, last_name, street_nbr, income 
FROM teradata.val.customer  
ORDER_BY income DESC 
LIMIT 10;
```


SELECT TOP 10 "cust_id", "income", "first_name", "last_name", "street_nbr" FROM "val"."customer" ORDER BY "income" ASC NULLS LAST


SELECT "cust_id", "income", "first_name", "last_name", "street_nbr" FROM "val"."customer"

## Join Queries

```sql
SELECT cus.cust_id
, cus.first_name
, cus.last_name
, acc.acct_type
, acc.starting_balance
, acc.ending_balance
FROM teradata.val.customer cus
LEFT JOIN teradata.val.accounts acc
ON (cus.cust_id = acc.cust_id)
```

```
Trino version: 414
Fragment 0 [HASH]
    Output layout: [cust_id, first_name, last_name, acct_type, account_active]
    Output partitioning: SINGLE []
    Output[columnNames = [cust_id, first_name, last_name, acct_type, account_active]]
    │   Layout: [cust_id:integer, first_name:char(30), last_name:char(30), acct_type:char(2), account_active:char(1)]
    │   Estimates: {rows: ? (?), cpu: 0, memory: 0B, network: 0B}
    └─ LeftJoin[criteria = ("cust_id" = "cust_id_0"), hash = [$hashvalue, $hashvalue_2], distribution = PARTITIONED]
       │   Layout: [cust_id:integer, first_name:char(30), last_name:char(30), acct_type:char(2), account_active:char(1)]
       │   Estimates: {rows: ? (?), cpu: ?, memory: ?, network: 0B}
       │   Distribution: PARTITIONED
       ├─ RemoteSource[sourceFragmentIds = [1]]
       │      Layout: [cust_id:integer, first_name:char(30), last_name:char(30), $hashvalue:bigint]
       └─ LocalExchange[partitioning = HASH, hashColumn = [$hashvalue_2], arguments = ["cust_id_0"]]
          │   Layout: [cust_id_0:integer, acct_type:char(2), account_active:char(1), $hashvalue_2:bigint]
          │   Estimates: {rows: ? (?), cpu: ?, memory: 0B, network: 0B}
          └─ RemoteSource[sourceFragmentIds = [2]]
                 Layout: [cust_id_0:integer, acct_type:char(2), account_active:char(1), $hashvalue_3:bigint]

Fragment 1 [SOURCE]
    Output layout: [cust_id, first_name, last_name, $hashvalue_1]
    Output partitioning: HASH [cust_id][$hashvalue_1]
    ScanProject[table = teradata:val.customer val.customer columns=[cust_id:integer:INTEGER, first_name:char(30):CHAR, last_name:char(30):CHAR]]
        Layout: [cust_id:integer, first_name:char(30), last_name:char(30), $hashvalue_1:bigint]
        Estimates: {rows: ? (?), cpu: ?, memory: 0B, network: 0B}/{rows: ? (?), cpu: ?, memory: 0B, network: 0B}
        $hashvalue_1 := combine_hash(bigint '0', COALESCE("$operator$hash_code"("cust_id"), 0))
        last_name := last_name:char(30):CHAR
        cust_id := cust_id:integer:INTEGER
        first_name := first_name:char(30):CHAR

Fragment 2 [SOURCE]
    Output layout: [cust_id_0, acct_type, account_active, $hashvalue_4]
    Output partitioning: HASH [cust_id_0][$hashvalue_4]
    ScanProject[table = teradata:val.accounts val.accounts columns=[cust_id:integer:INTEGER, acct_type:char(2):CHAR, account_active:char(1):CHAR]]
        Layout: [cust_id_0:integer, acct_type:char(2), account_active:char(1), $hashvalue_4:bigint]
        Estimates: {rows: ? (?), cpu: ?, memory: 0B, network: 0B}/{rows: ? (?), cpu: ?, memory: 0B, network: 0B}
        $hashvalue_4 := combine_hash(bigint '0', COALESCE("$operator$hash_code"("cust_id_0"), 0))
        cust_id_0 := cust_id:integer:INTEGER
        acct_type := acct_type:char(2):CHAR
        account_active := account_active:char(1):CHAR
```


## Aggregation Queries

### Count

**Trino Command**

```sql
SELECT t.age, count(*) from teradata.val.customer t GROUP BY t.age  
```

**Teradata Query**

```sql
SELECT "age", "_pfgnrtd_0" FROM (SELECT "age", count(*) AS "_pfgnrtd_0" FROM "val"."customer" GROUP BY "age") o
```           

**Trino Execution Plan**

```
Trino version: 414
Fragment 0 [SOURCE]
    Output layout: [age, _pfgnrtd]
    Output partitioning: SINGLE []
    Output[columnNames = [age, _col1]]
    │   Layout: [age:smallint, _pfgnrtd:bigint]
    │   Estimates: {rows: ? (?), cpu: 0, memory: 0B, network: 0B}
    │   _col1 := _pfgnrtd
    └─ TableScan[table = teradata:Query[SELECT "age", count(*) AS "_pfgnrtd_0" FROM "val"."customer" GROUP BY "age"] columns=[age:smallint:SMALLINT, _pfgnrtd_0:bigint:bigint]]
           Layout: [age:smallint, _pfgnrtd:bigint]
           Estimates: {rows: ? (?), cpu: ?, memory: 0B, network: 0B}
           _pfgnrtd := _pfgnrtd_0:bigint:bigint
           age := age:smallint:SMALLINT
```

### Count DISTINCT

**Trino Command**

```sql
SELECT COUNT (DISTINCT age) from teradata.val.customer
```

**Teradata Query**

```sql
SELECT "_pfgnrtd_0" FROM (SELECT count("age") AS "_pfgnrtd_0" FROM (SELECT "age" FROM "val"."customer" GROUP BY "age") o) o
```           

**Trino Execution Plan**

```
Trino version: 414
Fragment 0 [SOURCE]
    Output layout: [_pfgnrtd]
    Output partitioning: SINGLE []
    Output[columnNames = [_col0]]
    │   Layout: [_pfgnrtd:bigint]
    │   Estimates: {rows: 1 (9B), cpu: 0, memory: 0B, network: 0B}
    │   _col0 := _pfgnrtd
    └─ TableScan[table = teradata:Query[SELECT count("age") AS "_pfgnrtd_0" FROM (SELECT "age" FROM "val"."customer" GROUP BY "age") o] columns=[_pfgnrtd_0:bigint:bigint]]
           Layout: [_pfgnrtd:bigint]
           Estimates: {rows: 1 (9B), cpu: 9, memory: 0B, network: 0B}
           _pfgnrtd := _pfgnrtd_0:bigint:bigint
```

### Count ALL

**Trino Command**

```sql
SELECT COUNT (ALL age) from teradata.val.customer
```

**Teradata Query**

```sql
SELECT "_pfgnrtd_0" FROM (SELECT count("age") AS "_pfgnrtd_0" FROM "val"."customer") o
```           

**Trino Execution Plan**

```
Trino version: 414
Fragment 0 [SOURCE]
    Output layout: [_pfgnrtd]
    Output partitioning: SINGLE []
    Output[columnNames = [_col0]]
    │   Layout: [_pfgnrtd:bigint]
    │   Estimates: {rows: 1 (9B), cpu: 0, memory: 0B, network: 0B}
    │   _col0 := _pfgnrtd
    └─ TableScan[table = teradata:Query[SELECT count("age") AS "_pfgnrtd_0" FROM "val"."customer"] columns=[_pfgnrtd_0:bigint:bigint]]
           Layout: [_pfgnrtd:bigint]
           Estimates: {rows: 1 (9B), cpu: 9, memory: 0B, network: 0B}
           _pfgnrtd := _pfgnrtd_0:bigint:bigint
```


### Min

**Trino Command**

```sql
SELECT age, MIN(income) FROM teradata.val.customer c GROUP BY c.age
```

**Teradata Query**

```sql
SELECT "age", "_pfgnrtd_0" FROM (SELECT "age", min("income") AS "_pfgnrtd_0" FROM "val"."customer" GROUP BY "age") o
```

**Trino Execution Plan**

```
Trino version: 414
Fragment 0 [SOURCE]
    Output layout: [age, _pfgnrtd]
    Output partitioning: SINGLE []
    Output[columnNames = [age, _col1]]
    │   Layout: [age:smallint, _pfgnrtd:integer]
    │   Estimates: {rows: ? (?), cpu: 0, memory: 0B, network: 0B}
    │   _col1 := _pfgnrtd
    └─ TableScan[table = teradata:Query[SELECT "age", min("income") AS "_pfgnrtd_0" FROM "val"."customer" GROUP BY "age"] columns=[age:smallint:SMALLINT, _pfgnrtd_0:integer:INTEGER]]
           Layout: [age:smallint, _pfgnrtd:integer]
           Estimates: {rows: ? (?), cpu: ?, memory: 0B, network: 0B}
           _pfgnrtd := _pfgnrtd_0:integer:INTEGER
           age := age:smallint:SMALLINT
```

### Max

**Trino Command**

```sql
SELECT age, MAX(income) FROM teradata.val.customer c GROUP BY c.age
```

**Teradata Query**

```sql
SELECT "age", "_pfgnrtd_0" FROM (SELECT "age", max("income") AS "_pfgnrtd_0" FROM "val"."customer" GROUP BY "age") o
```

**Trino Execution Plan**

```
Trino version: 414
Fragment 0 [SOURCE]
    Output layout: [age, _pfgnrtd]
    Output partitioning: SINGLE []
    Output[columnNames = [age, _col1]]
    │   Layout: [age:smallint, _pfgnrtd:integer]
    │   Estimates: {rows: ? (?), cpu: 0, memory: 0B, network: 0B}
    │   _col1 := _pfgnrtd
    └─ TableScan[table = teradata:Query[SELECT "age", max("income") AS "_pfgnrtd_0" FROM "val"."customer" GROUP BY "age"] columns=[age:smallint:SMALLINT, _pfgnrtd_0:integer:INTEGER]]
           Layout: [age:smallint, _pfgnrtd:integer]
           Estimates: {rows: ? (?), cpu: ?, memory: 0B, network: 0B}
           _pfgnrtd := _pfgnrtd_0:integer:INTEGER
           age := age:smallint:SMALLINT
```

### AVG(bigint) 

(pushdown only for columns of type BIGINT, DOUBLE, DECIMAL, FLOAT, REAL)

**Trino Command**

```sql
SELECT id, avg(bigint_val) from teradata.hr.datatypes group by id 
```

**Teradata Query**

```sql
SELECT "id", "_pfgnrtd_0" FROM (SELECT "id", avg("bigint_val") AS "_pfgnrtd_0" FROM "HR"."datatypes" GROUP BY "id") o
```

**Trino Execution Plan**

```
Trino version: 414
Fragment 0 [SOURCE]
    Output layout: [id, _pfgnrtd]
    Output partitioning: SINGLE []
    Output[columnNames = [id, _col1]]
    │   Layout: [id:integer, _pfgnrtd:double]
    │   Estimates: {rows: ? (?), cpu: 0, memory: 0B, network: 0B}
    │   _col1 := _pfgnrtd
    └─ TableScan[table = teradata:Query[SELECT "id", avg("bigint_val") AS "_pfgnrtd_0" FROM "HR"."datatypes" GROUP BY "id"] columns=[id:integer:INTEGER, _pfgnrtd_0:double:double]]
           Layout: [id:integer, _pfgnrtd:double]
           Estimates: {rows: ? (?), cpu: ?, memory: 0B, network: 0B}
           _pfgnrtd := _pfgnrtd_0:double:double
           id := id:integer:INTEGER
```

### AVG(double) 

(pushdown only for columns of type BIGINT, DOUBLE, DECIMAL, FLOAT, REAL)

**Trino Command**

```sql
SELECT id, avg(double_val) from teradata.hr.datatypes group by id 
```

**Teradata Query**

```sql
SELECT "id", "_pfgnrtd_0" FROM (SELECT "id", avg("double_val") AS "_pfgnrtd_0" FROM "HR"."datatypes" GROUP BY "id") o
```

**Trino Execution Plan**

```
Trino version: 414
Fragment 0 [SOURCE]
    Output layout: [id, _pfgnrtd]
    Output partitioning: SINGLE []
    Output[columnNames = [id, _col1]]
    │   Layout: [id:integer, _pfgnrtd:double]
    │   Estimates: {rows: ? (?), cpu: 0, memory: 0B, network: 0B}
    │   _col1 := _pfgnrtd
    └─ TableScan[table = teradata:Query[SELECT "id", avg("double_val") AS "_pfgnrtd_0" FROM "HR"."datatypes" GROUP BY "id"] columns=[id:integer:INTEGER, _pfgnrtd_0:double:FLOAT]]
           Layout: [id:integer, _pfgnrtd:double]
           Estimates: {rows: ? (?), cpu: ?, memory: 0B, network: 0B}
           _pfgnrtd := _pfgnrtd_0:double:FLOAT
           id := id:integer:INTEGER
```

### AVG(decimal) 

(pushdown only for columns of type BIGINT, DOUBLE, DECIMAL, FLOAT, REAL)

**Trino Command**

```sql
SELECT id, avg(decimal_val) from teradata.hr.datatypes group by id 
```

**Teradata Query**

```sql
SELECT "id", "_pfgnrtd_0" FROM (SELECT "id", CAST(avg("decimal_val") AS decimal(5, 0)) AS "_pfgnrtd_0" FROM "HR"."datatypes" GROUP BY "id") o
```

**Trino Execution Plan**

```
Trino version: 414
Fragment 0 [SOURCE]
    Output layout: [id, _pfgnrtd]
    Output partitioning: SINGLE []
    Output[columnNames = [id, _col1]]
    │   Layout: [id:integer, _pfgnrtd:decimal(5,0)]
    │   Estimates: {rows: ? (?), cpu: 0, memory: 0B, network: 0B}
    │   _col1 := _pfgnrtd
    └─ TableScan[table = teradata:Query[SELECT "id", CAST(avg("decimal_val") AS decimal(5, 0)) AS "_pfgnrtd_0" FROM "HR"."datatypes" GROUP BY "id"] columns=[id:integer:INTEGER, _pfgnrtd_0:decimal(5,0):DECIMAL]]
           Layout: [id:integer, _pfgnrtd:decimal(5,0)]
           Estimates: {rows: ? (?), cpu: ?, memory: 0B, network: 0B}
           _pfgnrtd := _pfgnrtd_0:decimal(5,0):DECIMAL
           id := id:integer:INTEGER
```

### AVG(float) 

(pushdown only for columns of type BIGINT, DOUBLE, DECIMAL, FLOAT, REAL)

**Trino Command**

```sql
SELECT id, avg(float_val) from teradata.hr.datatypes group by id 
```

**Teradata Query**

```sql
SELECT "id", "_pfgnrtd_0" FROM (SELECT "id", avg("float_val") AS "_pfgnrtd_0" FROM "HR"."datatypes" GROUP BY "id") o
```

**Trino Execution Plan**

```
Trino version: 414
Fragment 0 [SOURCE]
    Output layout: [id, _pfgnrtd]
    Output partitioning: SINGLE []
    Output[columnNames = [id, _col1]]
    │   Layout: [id:integer, _pfgnrtd:double]
    │   Estimates: {rows: ? (?), cpu: 0, memory: 0B, network: 0B}
    │   _col1 := _pfgnrtd
    └─ TableScan[table = teradata:Query[SELECT "id", avg("float_val") AS "_pfgnrtd_0" FROM "HR"."datatypes" GROUP BY "id"] columns=[id:integer:INTEGER, _pfgnrtd_0:double:FLOAT]]
           Layout: [id:integer, _pfgnrtd:double]
           Estimates: {rows: ? (?), cpu: ?, memory: 0B, network: 0B}
           _pfgnrtd := _pfgnrtd_0:double:FLOAT
           id := id:integer:INTEGER
```


### AVG(real) 

(pushdown only for columns of type BIGINT, DOUBLE, DECIMAL, FLOAT, REAL)

**Trino Command**

```sql
SELECT id, avg(real_val) from teradata.hr.datatypes group by id 
```

**Teradata Query**

```sql
SELECT "id", "_pfgnrtd_0" FROM (SELECT "id", avg("real_val") AS "_pfgnrtd_0" FROM "HR"."datatypes" GROUP BY "id") o
```

**Trino Execution Plan**

```
Trino version: 414
Fragment 0 [SOURCE]
    Output layout: [id, _pfgnrtd]
    Output partitioning: SINGLE []
    Output[columnNames = [id, _col1]]
    │   Layout: [id:integer, _pfgnrtd:double]
    │   Estimates: {rows: ? (?), cpu: 0, memory: 0B, network: 0B}
    │   _col1 := _pfgnrtd
    └─ TableScan[table = teradata:Query[SELECT "id", avg("real_val") AS "_pfgnrtd_0" FROM "HR"."datatypes" GROUP BY "id"] columns=[id:integer:INTEGER, _pfgnrtd_0:double:FLOAT]]
           Layout: [id:integer, _pfgnrtd:double]
           Estimates: {rows: ? (?), cpu: ?, memory: 0B, network: 0B}
           _pfgnrtd := _pfgnrtd_0:double:FLOAT
           id := id:integer:INTEGER
```

### AVG(integer)  - no pushdown!!!

(pushdown only for columns of type BIGINT, DOUBLE, DECIMAL, FLOAT)

**Trino Command**

```sql
SELECT id, avg(integer_val) from teradata.hr.datatypes group by id 
```

**Teradata Query**

```sql
SELECT "id", "integer_val" FROM "HR"."datatypes"
```

**Trino Execution Plan**

```
Trino version: 414
Fragment 0 [HASH]
    Output layout: [id, avg]
    Output partitioning: SINGLE []
    Output[columnNames = [id, _col1]]
    │   Layout: [id:integer, avg:double]
    │   Estimates: {rows: ? (?), cpu: 0, memory: 0B, network: 0B}
    │   _col1 := avg
    └─ Project[]
       │   Layout: [id:integer, avg:double]
       │   Estimates: {rows: ? (?), cpu: ?, memory: 0B, network: 0B}
       └─ Aggregate[type = FINAL, keys = [id], hash = [$hashvalue]]
          │   Layout: [id:integer, $hashvalue:bigint, avg:double]
          │   Estimates: {rows: ? (?), cpu: ?, memory: ?, network: 0B}
          │   avg := avg("avg_1")
          └─ LocalExchange[partitioning = HASH, hashColumn = [$hashvalue], arguments = ["id"]]
             │   Layout: [id:integer, avg_1:row(double, bigint), $hashvalue:bigint]
             │   Estimates: {rows: ? (?), cpu: ?, memory: 0B, network: 0B}
             └─ RemoteSource[sourceFragmentIds = [1]]
                    Layout: [id:integer, avg_1:row(double, bigint), $hashvalue_2:bigint]

Fragment 1 [SOURCE]
    Output layout: [id, avg_1, $hashvalue_3]
    Output partitioning: HASH [id][$hashvalue_3]
    Aggregate[type = PARTIAL, keys = [id], hash = [$hashvalue_3]]
    │   Layout: [id:integer, $hashvalue_3:bigint, avg_1:row(double, bigint)]
    │   avg_1 := avg("integer_val_0")
    └─ ScanProject[table = teradata:hr.datatypes HR.datatypes columns=[id:integer:INTEGER, integer_val:integer:INTEGER]]
           Layout: [id:integer, integer_val_0:bigint, $hashvalue_3:bigint]
           Estimates: {rows: ? (?), cpu: ?, memory: 0B, network: 0B}/{rows: ? (?), cpu: ?, memory: 0B, network: 0B}
           integer_val_0 := CAST("integer_val" AS bigint)
           $hashvalue_3 := combine_hash(bigint '0', COALESCE("$operator$hash_code"("id"), 0))
           integer_val := integer_val:integer:INTEGER
           id := id:integer:INTEGER
```
