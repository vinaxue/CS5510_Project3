# CS5510_Project3

### External packages:

1. BTrees
2. pyparsing
3. fastapi, uvicorn \*for interface only

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
     - allow drop if any columns of the table is not referenced through foreign key
  3. CREATE INDEX
  4. DROP INDEX

- DML
  1. INSERT
  2. SELECT
     - JOIN (only two tables)
       - support join with self
       - cannot rename tables
     - Aggregation (GROUP BY)
       - MIN
       - MAX
       - SUM
     - Support conditions
  3. DELETE
     - Support conditions
  4. UPDATE
     - Support conditions
  5. Conditions (supports one or two joined by AND or OR)
     - '='
     - '>'
     - '<'

### Run unit tests:

`python -m unittest ./tests/{filename}.py` for entire test suite or `python -m unittest test.{filename}.{classname}.{testname}` for individual test

Ex:

- `python -m unittest ./tests/test_ddl_manager.py`
- `python -m unittest test.test_ddl_manager.DDLManagerTest.test_create_table`

### Start interfact:

1. Run `uvicorn app:app --reload` -> the API should start at `localhost:8000`
2. `cd interface`
3. `npm update install`
4. `npm start` -> the frontend should start at `localhost:3000`
