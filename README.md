# CS5510_Project3

### External packages:

1. BTrees
2. pyparsing
3. flask \*needs approval

### Supported features:

1. CREATE TABLE
   - primary key and foreign key support
2. DROP TABLE
   - doesn't allow drop if any columns of the table is referenced through foreign key

### Run unit tests:

`python -m unittest ./tests/{filename}.py`

### Run API:

Run `python app.py` -> API should start at `http://127.0.0.1:5000`
