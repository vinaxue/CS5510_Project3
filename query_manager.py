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
    Literal,
)

from ddl_manager import DDLManager
from dml_manager import DMLManager
from storage_manager import StorageManager
from utils import ASC, DESC, MAX, MIN, SUM, track_time


class QueryManager:
    def __init__(self, storage_manager, ddl_manager, dml_manager):
        self.storage_manager = storage_manager
        self.ddl_manager = ddl_manager
        self.dml_manager = dml_manager
        self.identifier = Word(alphas, alphanums + "_").setName("identifier")
        self.qualified_identifier = Combine(
            self.identifier + ZeroOrMore("." + self.identifier)
        )

        integer = Word(nums).setParseAction(lambda t: int(t[0]))
        float_literal = Combine(
            Optional(oneOf("+ -")) + Word(nums) + "." + Word(nums)
        ).setParseAction(lambda t: float(t[0]))
        self.numeric_literal = float_literal | Combine(Optional(oneOf("+ -")) + integer)
        # self.numeric_literal = Combine(Optional(oneOf("+ -")) + integer)
        self.string_literal = quotedString.setParseAction(removeQuotes)
        # self.constant = self.numeric_literal | self.string_literal
        # self.numeric_literal.setParseAction(
        #     lambda t: int(t[0]) if t[0].isdigit() else float(t[0])
        # )
        self.constant = float_literal | integer | self.string_literal
        # .setParseAction(
        #     lambda t: print(f"[CONSTANT PARSED]: {t[0]} ({type(t[0])})") or t
        # )

        self.ASC = CaselessKeyword("ASC")
        self.DESC = CaselessKeyword("DESC")

        star = Literal("*").setName("star")
        agg_func = Group(
            (
                CaselessKeyword("MIN")
                | CaselessKeyword("MAX")
                | CaselessKeyword("SUM")
                | CaselessKeyword("AVG")
                | CaselessKeyword("COUNT")
            )("func")
            + Suppress("(")
            + (self.qualified_identifier | star)("col")
            + Suppress(")")
        ).setResultsName("agg")

        order_spec = Group(
            self.qualified_identifier("col")
            + Optional(self.ASC("dir") | self.DESC("dir"))
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
            self.HAVING,
        ) = map(
            CaselessKeyword,
            """
            SELECT FROM WHERE GROUP BY ORDER INSERT INTO VALUES JOIN ON
            CREATE DROP INDEX TABLE PRIMARY KEY FOREIGN REFERENCES DELETE UPDATE SET HAVING
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
        identifier_or_agg = agg_func | self.qualified_identifier
        value_expr = self.constant | identifier_or_agg

        self.simple_condition = Group(
            identifier_or_agg("left")
            + oneOf("= > < >= <=")("operator")
            + value_expr("right")
        )
        # Allow chaining of simple_condition via AND/OR recursively
        self.condition <<= self.simple_condition + ZeroOrMore(
            (CaselessKeyword("AND") | CaselessKeyword("OR"))("logic") + self.condition
        )

        # Combine the WHERE keyword with the full recursive condition
        self.where_condition = Group(self.WHERE + self.condition)("where")
        # --- Multi-condition WHERE support ends ---
        self.group_by_clause = Group(self.GROUP + self.BY + self.column_list)
        self.having_clause = Group(self.HAVING + self.condition).setResultsName(
            "having"
        )
        self.order_by_clause = Group(
            self.ORDER + self.BY + delimitedList(order_spec)("order_specs")
        )

        select_list = (
            Group(delimitedList(agg_func | self.qualified_identifier))("select_cols")
            | "*"
        )

        self.select_stmt << (
            self.SELECT
            + select_list
            + self.FROM
            + self.table_with_joins
            + Optional(self.where_condition)
            + Optional(self.group_by_clause)
            + Optional(self.having_clause)
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
        val = raw
        # Convert raw literal to int/float if possible
        # try:
        #     val = int(raw)
        # except:
        #     try:
        #         val = float(raw)
        #     except:
        #         val = raw

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
        print(where_parse)
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
            elif cmd == "DROP":
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
            elif cmd == "INSERT":
                tbl = parsed[2]
                raw_vals = parsed.get("values") or []
                vals = []
                for v in raw_vals:
                    vals.append(v)
                self.dml_manager.insert(tbl, vals)
                continue

            # ----- SELECT -----
            elif cmd == "SELECT":
                print(parsed)
                sel = parsed[1]
                from_clause = parsed[3]
                left_tbl = from_clause[0]
                # Build where function if present
                where_tok = parsed.get("where")
                where_fn = self._build_where_fn(where_tok) if where_tok else None

                # Get order by tuples
                order_tok = None
                order_tuples = None
                for tok in parsed:
                    if (
                        len(tok) >= 2
                        and tok[0].upper() == "ORDER"
                        and tok[1].upper() == "BY"
                    ):
                        order_tok = tok
                        break
                if order_tok:
                    order_tuples = [
                        (col, DESC if direction.upper() == "DESC" else ASC)
                        for col, direction in order_tok[2:]
                    ]

                # Get group by
                group_tok = None
                group_by_col = []
                for tok in parsed:
                    if (
                        len(tok) >= 2
                        and tok[0].upper() == "GROUP"
                        and tok[1].upper() == "BY"
                    ):
                        group_tok = tok
                        break
                if group_tok:
                    for cols in group_tok[2]:
                        group_by_col.append(cols)

                # Get cols and aggregation
                cols = None if sel == "*" else []
                agg_func = []

                aggregation_function_map = {
                    "max": MAX,
                    "min": MIN,
                    "sum": SUM,
                }

                # Look for aggregation functions in the list of selected columns
                if cols is not None:
                    for tok in sel:
                        if isinstance(tok, str):
                            cols.append(tok)
                        elif tok[0].lower() in aggregation_function_map:
                            agg_func.append(
                                {aggregation_function_map[tok[0].lower()]: tok[1]}
                            )
                            cols.append(tok[1])

                # Get having if aggregation
                # if len(agg_func) > 0:
                #     having_tok = parsed.get("having")
                #     if having_tok:
                #         for condition in having_tok[1]:
                #             agg_col = condition[0][1]
                #             for agg in agg_func:
                #                 if (
                #                     agg_col in agg.values()
                #                 ):  # The column is part of an aggregation function
                #                     # Remove the aggregation function (e.g., 'SUM') and change to WHERE
                #                     condition[0] = (
                #                         agg_col  # Now the condition only has the column (e.g., 'Amount')
                #                     )

                #                     # Convert HAVING to WHERE by changing the keyword
                #                     parsed["having"] = ["WHERE"] + condition[
                #                         1:
                #                     ]  # Convert 'HAVING' to 'WHERE' with the condition
                #                     print(
                #                         f"Processed WHERE condition: {parsed['having']}"
                #                     )
                #                     break
                #     else:
                #         having_fn = None

                # print(having_fn)

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
                        order_by=order_tuples,
                        group_by=group_by_col if group_tok else None,
                        aggregates=agg_func if len(agg_func) > 0 else None,
                    )
                else:
                    result = self.dml_manager.select(
                        table_name=left_tbl,
                        columns=cols,
                        where=where_fn,
                        order_by=order_tuples,
                        group_by=group_by_col if group_tok else None,
                        aggregates=agg_func if len(agg_func) > 0 else None,
                    )
                return result

            # ----- DELETE -----
            elif cmd == "DELETE":
                tbl = parsed["table"]
                where_parse = parsed.get("where")
                base_where_fn = (
                    self._build_where_fn(where_parse) if where_parse else None
                )
                deleted_count = self.dml_manager.delete(tbl, base_where_fn)
                # print(f"Deleted {deleted_count} rows from {tbl}.")
                return deleted_count

            # ----- UPDATE -----
            elif cmd == "UPDATE":
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
                # print(f"Updated {count} rows in {tbl}.")
                return count

            # Unsupported command
            else:
                raise Exception(f"Unsupported SQL command: {cmd}")


# -*- coding: utf-8 -*-
if __name__ == "__main__":

    stor_mgr = StorageManager()
    ddl_mgr = DDLManager(stor_mgr)
    dml_mgr = DMLManager(stor_mgr)

    qm = QueryManager(stor_mgr, ddl_mgr, dml_mgr)

    multi_query = """
    SELECT dept, MIN(salary), count(*) FROM employees WHERE age > 30 GROUP BY dept HAVING MIN(salary) > 5000 and MAX(age) < 60;
 ;
    """

    try:
        parse_results = qm.parse_query(multi_query)
        for i, res in enumerate(parse_results, start=1):
            print(f"语句 {i} 解析成功:")
            print(res)
            print(type(res[4][1][2]))
            print("-" * 50)
    except Exception as e:
        print("解析错误:", e)
