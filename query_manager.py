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
        self.numeric_literal = float_literal | Combine(Optional(oneOf("+ -")) + integer)
        # self.numeric_literal = Combine(Optional(oneOf("+ -")) + integer)
        self.string_literal = quotedString.setParseAction(removeQuotes)
        self.constant = self.numeric_literal | self.string_literal

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
        """
        递归地把 tokens（ParseResults 或列表）转成一个 Python 函数 f(row)->bool。
        tokens 的格式是 [simple_cond, ('AND'|'OR'), subcond, ...]，
        simple_cond 本身是一个三元列表 [col, op, val]。
        """
        # simple part
        simple = tokens[0]
        col, op, raw = simple[0], simple[1], simple[2]
        # 把 raw 转成 Python 值
        try:
            val = int(raw)
        except:
            try:
                val = float(raw)
            except:
                val = raw
        # 构造最内层的函数
        if op == "=":
            funcs = [lambda row, c=col, v=val: row.get(c) == v]
        elif op == "<":
            funcs = [lambda row, c=col, v=val: row.get(c) < v]
        elif op == ">":
            funcs = [lambda row, c=col, v=val: row.get(c) > v]
        else:
            raise Exception(f"Unsupported operator: {op}")

        ops = []
        # 如果后面还有 AND/OR + subcondition
        idx = 1
        while idx < len(tokens):
            logic = tokens[idx].upper()
            sub = tokens[idx + 1]
            ops.append(logic)
            # 递归
            funcs.append(self._build_condition_fn(sub))
            idx += 2

        # 最终合并所有函数
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
        """
        where_parse 是 parse_query 得到的 where_condition，形如
        ['WHERE', cond_tokens...]
        跳过第 0 个 'WHERE'，把余下 tokens 传给 _build_condition_fn
        """
        cond_tokens = where_parse[1:]  # 去掉 'WHERE'
        return self._build_condition_fn(cond_tokens)

    @track_time
    def execute_query(self, query: str):
        """
        Execute the parsed query.

        """
        parsed_queries = self.parse_query(query)

        for parsed_query in parsed_queries:
            print(parsed_query)
            command = parsed_query[0]
            if command == "CREATE":
                if parsed_query[1] == "TABLE":
                    table_name, raw_columns = parsed_query[2:4]
                    columns = []
                    primary_key = None
                    foreign_keys = []

                    for col in raw_columns:
                        col_name, col_type = col[0], col[1]
                        constraints = col[2] if len(col) > 2 else []
                        columns.append((col_name, col_type))

                        if "PRIMARY KEY" in " ".join(constraints):
                            primary_key = col_name

                        if any(
                            "FOREIGN" in constraint for constraint in constraints
                        ) and any("KEY" in constraint for constraint in constraints):
                            ref_table = constraints[3]
                            ref_col = constraints[4]
                            foreign_keys.append((col_name, ref_table, ref_col))

                    self.ddl_manager.create_table(
                        table_name, columns, primary_key, foreign_keys
                    )

                elif parsed_query[1] == "INDEX":
                    index_name = parsed_query[2]
                    table_name = parsed_query[4]
                    column = parsed_query[5]

                    self.ddl_manager.create_index(table_name, column[0], index_name)

            elif command == "DROP":
                if parsed_query[1] == "TABLE":
                    table_name = parsed_query[2]
                    self.ddl_manager.drop_table(table_name)
                elif parsed_query[1] == "INDEX":
                    index_name = parsed_query[2]
                    self.ddl_manager.drop_index(index_name)

            elif command == "INSERT":
                table_name = parsed_query[2]  # The table name
                columns = parsed_query[3]  # The list of columns
                values = [
                    (
                        int(value)
                        if value.isdigit()
                        else (
                            float(value)
                            if value.replace(".", "", 1).isdigit()
                            else value
                        )
                    )
                    for value in parsed_query[5]
                ]  # Convert values to int or float if applicable
                self.dml_manager.insert(table_name, values)

            elif command == "SELECT":
                selected_columns = parsed_query[1] if parsed_query[1] != "*" else None
                from_clause = parsed_query[3]

                table_name = from_clause[0]
                where_function = None

                where_flag = False
                # Process WHERE conditions, if any
                where_condition = parsed_query[4] if len(parsed_query) > 4 else None
                if where_condition:
                    where_flag = True
                    where_column = where_condition[1]
                    where_operator = where_condition[2]
                    where_value = (
                        int(where_condition[3])
                        if where_condition[3].isdigit()
                        else (
                            float(where_condition[3])
                            if where_condition[3].replace(".", "", 1).isdigit()
                            else where_condition[3]
                        )
                    )

                    if "." in where_column:
                        table_prefix, column_name = where_column.split(".")
                        key_name = f"{table_prefix}.{column_name}"
                    else:

                        key_name = (
                            where_column
                            if len(from_clause) == 1
                            else f"{table_name}.{where_column}"
                        )

                    print("Using where key:", key_name)

                    if where_operator == "=":
                        where_function = lambda row: row.get(key_name) == where_value
                    elif where_operator == ">":
                        where_function = lambda row: row.get(key_name) > where_value
                    elif where_operator == "<":
                        where_function = lambda row: row.get(key_name) < where_value
                    else:
                        raise Exception(
                            f"Unsupported condition operator: {where_operator}"
                        )

                # Process JOINs, if any
                if len(from_clause) > 1:
                    join_conditions = from_clause[1]

                    if join_conditions[0] == "JOIN":
                        right_table = join_conditions[1]
                        left_column = join_conditions[2][1].split(".")[-1]
                        right_column = join_conditions[2][3].split(".")[-1]

                    results = self.dml_manager.select_join_with_index(
                        left_table=table_name,
                        right_table=right_table,
                        left_join_col=left_column,
                        right_join_col=right_column,
                        columns=selected_columns,
                        where=where_function,
                    )
                else:
                    results = self.dml_manager.select(
                        table_name=table_name,
                        columns=selected_columns,
                        where=(
                            [where_column, where_operator, where_value]
                            if where_flag
                            else None
                        ),
                    )
                return results

            elif command == "DELETE":
                # DELETE FROM <table> [WHERE condition]
                table_name = parsed_query["table"]
                where_function = None

                if "where" in parsed_query:
                    where_condition = parsed_query["where"]
                    where_column = where_condition[1]
                    where_operator = where_condition[2]
                    where_value = (
                        int(where_condition[3])
                        if where_condition[3].isdigit()
                        else (
                            float(where_condition[3])
                            if where_condition[3].replace(".", "", 1).isdigit()
                            else where_condition[3]
                        )
                    )

                    if "." in where_column:
                        table_for_where, column_name = where_column.split(".")
                    else:
                        table_for_where = table_name
                        column_name = where_column

                    db = self.storage_manager.db
                    table_columns = db["COLUMNS"][table_for_where]
                    column_names = list(table_columns.keys())
                    where_column_index = column_names.index(column_name)

                    if where_operator == "=":
                        where_function = (
                            lambda row: row[where_column_index] == where_value
                        )
                    elif where_operator == ">":
                        where_function = (
                            lambda row: row[where_column_index] > where_value
                        )
                    elif where_operator == "<":
                        where_function = (
                            lambda row: row[where_column_index] < where_value
                        )
                    else:
                        raise Exception(f"Unsupported operator: {where_operator}")
                delete_count = self.dml_manager.delete(table_name, where_function)
                print(f"Deleted {delete_count} rows from table {table_name}.")

            elif command == "UPDATE":

                table_name = parsed_query["table"]

                updates = {}
                for update_item in parsed_query["updates"]:
                    col = update_item["col"]
                    val = update_item["val"]

                    if isinstance(val, str):
                        if val.isdigit():
                            val = int(val)
                        else:
                            try:
                                val = float(val)
                            except ValueError:
                                pass
                    updates[col] = val

                where_function = None
                if "where" in parsed_query:
                    where_condition = parsed_query["where"]
                    where_column = where_condition[1]
                    where_operator = where_condition[2]
                    where_value = (
                        int(where_condition[3])
                        if where_condition[3].isdigit()
                        else (
                            float(where_condition[3])
                            if where_condition[3].replace(".", "", 1).isdigit()
                            else where_condition[3]
                        )
                    )
                    if "." in where_column:
                        table_for_where, column_name = where_column.split(".")
                    else:
                        table_for_where = table_name
                        column_name = where_column

                    db = self.storage_manager.db
                    table_columns = db["COLUMNS"][table_for_where]
                    column_names = list(table_columns.keys())
                    where_column_index = column_names.index(column_name)

                    if where_operator == "=":
                        where_function = (
                            lambda row: row[where_column_index] == where_value
                        )
                    elif where_operator == ">":
                        where_function = (
                            lambda row: row[where_column_index] > where_value
                        )
                    elif where_operator == "<":
                        where_function = (
                            lambda row: row[where_column_index] < where_value
                        )
                    else:
                        raise Exception(f"Unsupported operator: {where_operator}")

                update_count = self.dml_manager.update(
                    table_name, updates, where_function
                )
                print(f"Updated {update_count} rows in table {table_name}.")

            else:
                raise Exception("Unsupported SQL command")


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
