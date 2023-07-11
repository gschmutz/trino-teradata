# Trino Teradata Connector

## Data Structure for Tests

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
select * from teradata.val.accounts 
```

returns an error

```
SQL Error [67108864]: Query failed (#20230706_102512_00082_8jbns): Driver returned null LocalDate for a non-null value
```

## Simple Where Queries

### SELECT on cust_id (PK)

```sql
SELECT cust_id, first_name, last_name FROM teradata.val.customer WHERE cust_id = 1362691
```

```
Trino version: 414
Fragment 0 [SOURCE]
    Output layout: [cust_id, first_name, last_name]
    Output partitioning: SINGLE []
    Output[columnNames = [cust_id, first_name, last_name]]
    │   Layout: [cust_id:integer, first_name:char(30), last_name:char(30)]
    │   Estimates: {rows: ? (?), cpu: 0, memory: 0B, network: 0B}
    └─ TableScan[table = teradata:val.customer val.customer constraint on [cust_id] columns=[cust_id:integer:INTEGER, first_name:char(30):CHAR, last_name:char(30):CHAR]]
           Layout: [cust_id:integer, first_name:char(30), last_name:char(30)]
           Estimates: {rows: ? (?), cpu: ?, memory: 0B, network: 0B}
           last_name := last_name:char(30):CHAR
           cust_id := cust_id:integer:INTEGER
           first_name := first_name:char(30):CHAR
```

### SELECT on first_name

```sql
SELECT cust_id, first_name, last_name FROM teradata.val.customer WHERE first_name = 'Donald'
```

```
Trino version: 414
Fragment 0 [SOURCE]
    Output layout: [cust_id, first_name, last_name]
    Output partitioning: SINGLE []
    Output[columnNames = [cust_id, first_name, last_name]]
    │   Layout: [cust_id:integer, first_name:char(30), last_name:char(30)]
    │   Estimates: {rows: ? (?), cpu: 0, memory: 0B, network: 0B}
    └─ TableScan[table = teradata:val.customer val.customer constraint on [first_name] columns=[cust_id:integer:INTEGER, first_name:char(30):CHAR, last_name:char(30):CHAR]]
           Layout: [cust_id:integer, first_name:char(30), last_name:char(30)]
           Estimates: {rows: ? (?), cpu: ?, memory: 0B, network: 0B}
           last_name := last_name:char(30):CHAR
           cust_id := cust_id:integer:INTEGER
           first_name := first_name:char(30):CHAR
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


```sql
SELECT t.age, count(*) from teradata.val.customer t GROUP BY t.age  
```

```txt
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