# CS5510_Project3

### External packages:

1. BTrees
2. pyparsing
3. fastapi, uvicorn \*needs approval

### Supported features:

- Data types:

  - int
  - string
  - double

- DDL

  1. CREATE TABLE
     - primary key and foreign key support
     - default index on the primary key
  2. DROP TABLE
     - allow drop if any columns of the table is referenced through foreign key
  3. CREATE INDEX
  4. DROP INDEX

- DML
  1. INSERT
  2. SELECT
     1. JOIN (only two tables)
     2. Conditions (only 1 for now)
        - '='
        - '>'
        - '<'
     3. Aggregation (GROUP BY)
        - MIN
        - MAX
        - SUM
  3. DELETE
  4. UPDATE

### Run unit tests:

`python -m unittest ./tests/{filename}.py`

### Start interfact:

1. Run `uvicorn app:app --reload` -> the API should start at `localhost:8000`
2. `cd interface`
3. `npm update install`
4. `npm start` -> the frontend should start at `localhost:3000`
