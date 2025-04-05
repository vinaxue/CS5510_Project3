# CS5510_Project3

### External packages:

1. BTrees
2. pyparsing
3. fastapi, uvicorn \*needs approval

### Supported features:

1. CREATE TABLE
   - primary key and foreign key support
2. DROP TABLE
   - doesn't allow drop if any columns of the table is referenced through foreign key

### Run unit tests:

`python -m unittest ./tests/{filename}.py`

### Start interfact:

1. Run `uvicorn app:app --reload` -> the API should start at `localhost:8000`
2. `cd interface`
3. `npm update install`
4. `npm start` -> the frontend should start at `localhost:3000`
