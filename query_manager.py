from pyparsing import (
    CaselessKeyword,
    Word,
    alphas,
    alphanums,
    delimitedList,
    Group,
    Optional,
    Forward,
    oneOf,
    quotedString,
    removeQuotes,
    Suppress,
    nums,
    Combine,
    OneOrMore,
    ZeroOrMore,
)

from ddl_manager import DDLManager
from dml_manager import DMLManager
from storage_manager import StorageManager
from utils import track_time
import time
from functools import wraps


def track_time(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        elapsed = (time.time() - start) * 1000
        print(f"{func.__name__} executed in {elapsed:.2f} ms")
        return result

    return wrapper


class QueryManager:
    def __init__(self, storage_manager, ddl_manager, dml_manager):
        self.storage_manager = storage_manager
        self.ddl_manager = ddl_manager
        self.dml_manager = dml_manager
        self.identifier = Word(alphas, alphanums + "_").setName("identifier")
        self.qualified_identifier = Combine(
            self.identifier + ZeroOrMore("." + self.identifier)
        )

        integer = Word(nums)
        float_literal = Combine(Optional(oneOf("+ -")) + Word(nums) + "." + Word(nums))
        self.numeric_literal = (float_literal | Combine(Optional(oneOf("+ -")) + integer)
    )
        # self.numeric_literal = Combine(Optional(oneOf("+ -")) + integer)
        self.string_literal = quotedString.setParseAction(removeQuotes)
        self.constant = self.numeric_literal | self.string_literal
        self.numeric_literal.setParseAction(
        lambda t: int(t[0]) if t[0].isdigit() else float(t[0])
    )

        (
            self.SELECT,
            self.FROM,
            self.WHERE,
            self.GROUP,
            self.BY,
            self.ORDER,
            self.INSERT,
            self.INTO,
            self.VALUES,
            self.JOIN,
            self.ON,
            self.CREATE,
            self.DROP,
            self.INDEX,
            self.TABLE,
            self.PRIMARY,
            self.KEY,
            self.FOREIGN,
            self.REFERENCES,
            self.DELETE,
            self.UPDATE,
            self.SET,
        ) = map(
            CaselessKeyword,
            """
            SELECT FROM WHERE GROUP BY ORDER INSERT INTO VALUES JOIN ON
            CREATE DROP INDEX TABLE PRIMARY KEY FOREIGN REFERENCES DELETE UPDATE SET
            """.split(),
        )

        self.column_name = self.qualified_identifier
        self.column_list = Group(delimitedList(self.column_name))
        self.table_name = self.identifier

        self.join_condition = Group(
            self.ON + self.qualified_identifier + "=" + self.qualified_identifier
        )
        self.join_clause = Group(self.JOIN + self.table_name + self.join_condition)
        self.table_with_joins = Group(
            self.table_name + Optional(OneOrMore(self.join_clause))
        )

        self.select_stmt = Forward()

        # --- Multi-condition WHERE support begins ---
        # Placeholder for recursive condition definition
        self.condition = Forward()

        # Define a single simple condition: qualified_identifier operator (constant | qualified_identifier)
        self.simple_condition = Group(
            self.qualified_identifier("left")
            + oneOf("= > < >= <=")("operator")
            + (self.constant | self.qualified_identifier)("right")
        )

        # Allow chaining of simple_condition via AND/OR recursively
        self.condition <<= self.simple_condition + ZeroOrMore(
            (CaselessKeyword("AND") | CaselessKeyword("OR"))("logic") + self.condition
        )

        # Combine the WHERE keyword with the full recursive condition
        self.where_condition = Group(self.WHERE + self.condition)("where")
        # --- Multi-condition WHERE support ends ---

        self.group_by_clause = Group(self.GROUP + self.BY + self.column_list)
        self.order_by_clause = Group(self.ORDER + self.BY + self.column_list)

        self.select_stmt << (
            self.SELECT
            + (Group(delimitedList(self.qualified_identifier)) | "*")
            + self.FROM
            + self.table_with_joins
            + Optional(self.where_condition)
            + Optional(self.group_by_clause)
            + Optional(self.order_by_clause)
        )

        self.insert_stmt = (
            self.INSERT
            + self.INTO
            + self.table_name("table")
            + Optional(
                Suppress("(")
                + Group(delimitedList(self.identifier))("columns")
                + Suppress(")")
            )
            + self.VALUES
            + Suppress("(")
            + Group(delimitedList(self.constant))("values")
            + Suppress(")")
        )

        self.create_index_stmt = self.CREATE + self.INDEX + self.identifier(
            "index_name"
        ) + self.FROM.suppress() ^ (
            self.CREATE
            + self.INDEX
            + self.identifier("index_name")
            + self.table_name("on_table")
        )

        self.create_index_stmt = (
            self.CREATE
            + self.INDEX
            + self.identifier("index_name")
            + CaselessKeyword("ON")
            + self.table_name("on_table")
            + Suppress("(")
            + self.column_list("columns")
            + Suppress(")")
        )

        self.drop_index_stmt = (
            self.DROP
            + self.INDEX
            + self.identifier("index_name")
            + Optional(CaselessKeyword("ON") + self.table_name("on_table"))
        )

        self.double_type = CaselessKeyword("double")
        self.int_type = CaselessKeyword("int")
        self.string_type = CaselessKeyword("string")
        self.column_type = self.int_type | self.string_type | self.double_type

        self.primary_key_clause = Group(self.PRIMARY + self.KEY)

        self.foreign_key_clause = Group(
            self.FOREIGN
            + self.KEY
            + self.REFERENCES
            + self.table_name("ref_table")
            + Suppress("(")
            + self.identifier("ref_col")
            + Suppress(")")
        )

        self.column_definition = Group(
            self.identifier("col_name")
            + self.column_type("col_type")
            + Optional(self.primary_key_clause("pk"))
            + Optional(self.foreign_key_clause("fk"))
        )

        self.create_table_stmt = (
            self.CREATE
            + self.TABLE
            + self.table_name("table_name")
            + Suppress("(")
            + Group(delimitedList(self.column_definition))("columns")
            + Suppress(")")
        )

        self.delete_stmt = (
            self.DELETE
            + self.FROM
            + self.table_name("table")
            + Optional(self.where_condition("where"))
        )

        self.drop_table_stmt = self.DROP + self.TABLE + self.table_name("table_name")

        self.update_stmt = (
            self.UPDATE
            + self.table_name("table")
            + self.SET
            + Group(
                delimitedList(
                    Group(
                        self.identifier("col")
                        + Suppress("=")
                        + (self.constant | self.qualified_identifier)("val")
                    )
                )
            )("updates")
            + Optional(self.where_condition("where"))  # 支持可选的 WHERE 子句
        )

        self.sql_stmt = (
            self.select_stmt("select")
            | self.insert_stmt("insert")
            | self.create_index_stmt("create_index")
            | self.drop_index_stmt("drop_index")
            | self.create_table_stmt("create_table")
            | self.drop_table_stmt("drop_table")
            | self.delete_stmt("delete")
            | self.update_stmt("update")
        )

    def parse_query(self, queries: str):

        statements = [stmt.strip() for stmt in queries.split(";") if stmt.strip()]
        results = []
        for stmt in statements:
            try:
                parsed_result = self.sql_stmt.parseString(stmt, parseAll=True)
                results.append(parsed_result)
            except Exception as e:
                raise Exception(
                    f"Query parsing error in statement '{stmt}': {e}"
                ) from e
        return results

    def _build_condition_fn(self, tokens):
        # print(tokens)
        """
        Recursively build a filter function f(row_dict)->bool from tokens,
        where tokens is either:
          - ['col', 'op', val]        (simple condition)
          - [simple_cond, logic, sub, ...] (chained)
        """
        # --- Detect pure simple-condition case ---
        # tokens might be a ParseResults or list of exactly 3 strings/numbers
        if len(tokens) == 3 and isinstance(tokens[0], str):
            simple = tokens  # ['col', 'op', raw]
            rest = []
        else:
            # tokens[0] is a simple_condition group, rest are [logic, subcond, logic, subcond...]
            simple = tokens[0]
            rest = tokens[1:]

        # Extract column, operator, raw value
        col, op, raw = simple[0], simple[1], simple[2]
        # Convert raw literal to int/float if possible
        try:
            val = int(raw)
        except:
            try:
                val = float(raw)
            except:
                val = raw

        # Build the first (innermost) comparison function
        if op == "=":
            funcs = [lambda row, c=col, v=val: row.get(c) == v]
        elif op == "!=":
            funcs = [lambda row, c=col, v=val: row.get(c) != v]
        elif op == "<":
            funcs = [lambda row, c=col, v=val: row.get(c) < v]
        elif op == ">":
            funcs = [lambda row, c=col, v=val: row.get(c) > v]
        elif op == "<=":
            funcs = [lambda row, c=col, v=val: row.get(c) <= v]
        elif op == ">=":
            funcs = [lambda row, c=col, v=val: row.get(c) >= v]
        else:
            raise Exception(f"Unsupported operator: {op}")

        # Now handle any chained (logic, subcondition) in rest
        ops = []
        idx = 0
        while idx < len(rest):
            logic = rest[idx].upper()  # "AND"/"OR"
            sub = rest[idx + 1]
            ops.append(logic)
            funcs.append(self._build_condition_fn(sub))
            idx += 2

        # Combine all funcs by AND/OR
        def where_fn(row):
            res = funcs[0](row)
            for logic, f in zip(ops, funcs[1:]):
                if logic == "AND":
                    res = res and f(row)
                else:
                    res = res or f(row)
            return res

        return where_fn

    def _build_where_fn(self, where_parse):
        cond_tokens = where_parse[1:]
        return self._build_condition_fn(cond_tokens)

    @track_time
    def execute_query(self, query: str):
        """
        Execute a SQL statement. Supports:
        - CREATE TABLE / CREATE INDEX
        - DROP TABLE / DROP INDEX
        - INSERT
        - SELECT (with optional JOIN and WHERE)
        - DELETE
        - UPDATE
        """
        # Parse one or more statements
        parsed_queries = self.parse_query(query)

        for parsed in parsed_queries:
            cmd = parsed[0].upper()
            where_tok = parsed.get("where")
            print("DEBUG where_tok:", where_tok)
            base_where_fn = self._build_where_fn(where_tok) if where_tok else None
            # ----- CREATE -----
            if cmd == "CREATE":
                # CREATE TABLE
                if parsed[1].upper() == "TABLE":
                    table_name, raw_cols = parsed[2:4]
                    cols = []
                    pk = None
                    fks = []
                    # Extract column defs, PK and FK
                    for col in raw_cols:
                        name, ctype = col[0], col[1]
                        cols.append((name, ctype))
                        constraints = col[2] if len(col) > 2 else []
                        if "PRIMARY KEY" in " ".join(constraints):
                            pk = name
                        if any("FOREIGN" in c for c in constraints) and any(
                            "KEY" in c for c in constraints
                        ):
                            ref_table = constraints[3]
                            ref_col = constraints[4]
                            fks.append((name, ref_table, ref_col))
                    self.ddl_manager.create_table(table_name, cols, pk, fks)

                # CREATE INDEX
                elif parsed[1].upper() == "INDEX":
                    idx_name = parsed[2]
                    tbl = parsed[4]
                    col = parsed[5][0]
                    self.ddl_manager.create_index(tbl, col, idx_name)
                continue

            # ----- DROP -----
            if cmd == "DROP":
                # DROP TABLE
                if parsed[1].upper() == "TABLE":
                    tbl = parsed[2]
                    self.ddl_manager.drop_table(tbl)
                # DROP INDEX
                elif parsed[1].upper() == "INDEX":
                    idx_name = parsed[2]
                    self.ddl_manager.drop_index(idx_name)
                continue

            # ----- INSERT -----
            if cmd == "INSERT":
                tbl = parsed[2]
                raw_vals = parsed.get("values") or []
                # Convert to int/float if possible
                vals = []
                for v in raw_vals:
                    if isinstance(v, str) and v.isdigit():
                        vals.append(int(v))
                    elif isinstance(v, str) and v.replace(".", "", 1).isdigit():
                        vals.append(float(v))
                    else:
                        vals.append(v)
                self.dml_manager.insert(tbl, vals)
                continue

            # ----- SELECT -----
            if cmd == "SELECT":
                sel = parsed[1]
                cols = None if sel == "*" else list(sel)
                from_clause = parsed[3]
                left_tbl = from_clause[0]
                # Build where function if present
                where_tok = parsed.get("where")
                where_fn = self._build_where_fn(where_tok) if where_tok else None

                # Check for JOIN
                if len(from_clause) > 1:
                    join = from_clause[1]
                    right_tbl = join[1]
                    cond = join[2]
                    left_col = cond[1].split(".")[-1]
                    right_col = cond[3].split(".")[-1]
                    result = self.dml_manager.select_join_with_index(
                        left_table=left_tbl,
                        right_table=right_tbl,
                        left_join_col=left_col,
                        right_join_col=right_col,
                        columns=cols,
                        where=where_fn,
                    )
                else:
                    result = self.dml_manager.select(
                        table_name=left_tbl,
                        columns=cols,
                        where=where_fn,
                    )
                return result

            # ----- DELETE -----
            if cmd == "DELETE":
                tbl = parsed["table"]
                where_parse = parsed.get("where")
                base_where_fn = (
                    self._build_where_fn(where_parse) if where_parse else None
                )
                deleted_count = self.dml_manager.delete(tbl, base_where_fn)
                print(f"Deleted {deleted_count} rows from {tbl}.")
                return deleted_count

            # ----- UPDATE -----
            if cmd == "UPDATE":
                tbl = parsed.get("table")
                # Parse SET clauses
                updates = {}
                for u in parsed["updates"]:
                    c, v = u["col"], u["val"]
                    if isinstance(v, str) and v.isdigit():
                        v = int(v)
                    elif isinstance(v, str) and v.replace(".", "", 1).isdigit():
                        v = float(v)
                    updates[c] = v
                # Build where function if present
                where_tok = parsed.get("where")
                where_fn = self._build_where_fn(where_tok) if where_tok else None
                count = self.dml_manager.update(tbl, updates, where_fn)
                print(f"Updated {count} rows in {tbl}.")
                return

            # Unsupported command
            raise Exception(f"Unsupported SQL command: {cmd}")


# -*- coding: utf-8 -*-
if __name__ == "__main__":

    stor_mgr = StorageManager()
    ddl_mgr = DDLManager(stor_mgr)
    dml_mgr = DMLManager(stor_mgr)

    qm = QueryManager(stor_mgr, ddl_mgr, dml_mgr)

    multi_query = """
    SELECT * FROM employees WHERE age >= 30 AND salary < 5000 OR department = 'Sales';
    SELECT * FROM employees ORDER BY name;
    """

    try:
        parse_results = qm.parse_query(multi_query)
        for i, res in enumerate(parse_results, start=1):
            print(f"语句 {i} 解析成功:")
            print(res)
            print("-" * 50)
    except Exception as e:
        print("解析错误:", e)

    class QueryManagerTest:
        def __init__(self):
            self.identifier = Word(alphas, alphanums + "_")
            self.qualified_identifier = Combine(
                self.identifier + ZeroOrMore("." + self.identifier)
            )
            integer = Word(nums)
            float_literal = Combine(
                Optional(oneOf("+ -")) + Word(nums) + "." + Word(nums)
            )
            self.numeric_literal = float_literal | Combine(
                Optional(oneOf("+ -")) + integer
            )
            self.string_literal = quotedString.setParseAction(removeQuotes)
            self.constant = self.numeric_literal | self.string_literal
            self.numeric_literal.setParseAction(
                lambda t: int(t[0]) if t[0].isdigit() else float(t[0])
            )
            self.SELECT, self.FROM, self.WHERE = map(
                CaselessKeyword, "SELECT FROM WHERE".split()
            )

            self.simple_condition = Group(
                self.qualified_identifier("left")
                + oneOf("= > < >= <=")("operator")
                + (self.constant | self.qualified_identifier)("right")
            )
            self.condition = Forward()
            self.condition <<= self.simple_condition + ZeroOrMore(
                (CaselessKeyword("AND") | CaselessKeyword("OR"))("logic")
                + self.condition
            )
            self.where_condition = Group(self.WHERE + self.condition)("where")

            self.select_stmt = Forward()
            self.select_stmt <<= (
                self.SELECT
                + "*"
                + self.FROM
                + self.identifier("table")
                + Optional(self.where_condition)
            )
            self.sql_stmt = self.select_stmt

        def parse_query(self, sql):
            return [self.sql_stmt.parseString(sql, parseAll=True)]

    qm = QueryManagerTest()
    tests = [
        ("unquoted 1", "SELECT * FROM t WHERE col = 1"),
        ("quoted '1'", "SELECT * FROM t WHERE col = '1'"),
    ]

    for desc, sql in tests:
        parsed = qm.parse_query(sql)[0]
        where = parsed.get("where")
        cond = where[1]
        literal = cond[2]
        print(f"{desc}: literal={literal!r}, type={type(literal).__name__}")
