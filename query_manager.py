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


class QueryManager:
    def __init__(self, ddl_manager, dml_manager):
        self.ddl_manager = ddl_manager
        self.dml_manager = dml_manager

        self.identifier = Word(alphas, alphanums + "_").setName("identifier")
        self.qualified_identifier = Combine(
            self.identifier + ZeroOrMore("." + self.identifier)
        )
        integer = Word(nums)
        self.numeric_literal = Combine(Optional(oneOf("+ -")) + integer)
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
        ) = map(
            CaselessKeyword,
            """
            SELECT FROM WHERE GROUP BY ORDER INSERT INTO VALUES JOIN ON
            CREATE DROP INDEX TABLE PRIMARY KEY FOREIGN REFERENCES
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

        self.where_condition = Group(
            self.WHERE
            + self.qualified_identifier
            + oneOf("= > < >= <=")
            + (self.constant | self.qualified_identifier)
        )
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

        self.drop_table_stmt = self.DROP + self.TABLE + self.table_name("table_name")
        self.sql_stmt = (
            self.select_stmt("select")
            | self.insert_stmt("insert")
            | self.create_index_stmt("create_index")
            | self.drop_index_stmt("drop_index")
            | self.create_table_stmt("create_table")
            | self.drop_table_stmt("drop_table")
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

    def execute_query(self, query: str):
        """
        Execute the parsed query.

        """
        parsed_query = self.parse_query(query)

        command = parsed_query[0]
        print(parsed_query)

        if command == "CREATE":
            if parsed_query[1] == "TABLE":
                table_name, raw_columns = parsed_query[2:]
                columns = []
                primary_key = None
                foreign_keys = []

                for col in raw_columns:
                    col_name, col_type = col[0], col[1]
                    constraints = col[2] if len(col) > 2 else []
                    columns.append((col_name, col_type))

                    if "PRIMARY KEY" in " ".join(constraints):
                        primary_key = col_name

                    if "FOREIGN" in constraints and "KEY" in constraints:
                        ref_index = constraints.index("REFERENCES")
                        ref_table = constraints[ref_index + 1]
                        ref_col = constraints[ref_index + 2]
                        foreign_keys.append((col_name, ref_table, ref_col))

                self.ddl_manager.create_table(
                    table_name, columns, primary_key, foreign_keys
                )

            elif "INDEX" in parsed_query:
                index_name = parsed_query.index_name
                table_name = parsed_query.on_table
                columns = [col for col in parsed_query.columns]
                self.ddl_manager.create_index(table_name, columns, index_name)
        elif command == "DROP":
            if "TABLE" in parsed_query:
                table_name = parsed_query.table_name
                self.ddl_manager.drop_table(table_name)
            elif "INDEX" in parsed_query:
                index_name = parsed_query.index_name
                self.ddl_manager.drop_index(index_name)
        elif command == "INSERT":
            table_name = parsed_query.table
            columns = parsed_query.columns.asList() if parsed_query.columns else None
            values = parsed_query.values.asList()
            self.dml_manager.insert(table_name, columns, values)
        elif command == "SELECT":
            columns = parsed_query[1]
            from_part = parsed_query[3]
            table_name = from_part[0]
            if isinstance(from_part[1], list) and from_part[1][0] == "JOIN":
                join_table = from_part[1][1]
                on_clause = from_part[1][2]
                left_join_col = on_clause[1].split(".")[1]
                right_join_col = on_clause[3].split(".")[1]

                where_clause = None
                for clause in parsed_query[4:]:
                    if clause[0] == "WHERE":
                        where_clause = clause[1:]

                self.dml_manager.select_with_join(
                    left_table=table_name,
                    right_table=join_table,
                    left_join_col=left_join_col,
                    right_join_col=right_join_col,
                    columns=columns,
                    where=where_clause,
                )
            else:
                where_clause = None
                for clause in parsed_query[4:]:
                    if clause[0] == "WHERE":
                        where_clause = clause[1:]

                self.dml_manager.select_from_table(
                    table_name=table_name,
                    columns=columns,
                    where=where_clause,
                )
        else:
            raise Exception("Unsupported SQL command")

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
                    # Handle CREATE TABLE
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

                        if "FOREIGN" in constraints and "KEY" in constraints:
                            ref_index = constraints.index("REFERENCES")
                            ref_table = constraints[ref_index + 1]
                            ref_col = constraints[ref_index + 2]
                            foreign_keys.append((col_name, ref_table, ref_col))

                    self.ddl_manager.create_table(
                        table_name, columns, primary_key, foreign_keys
                    )

                elif parsed_query[1] == "INDEX":
                    # Handle CREATE INDEX
                    index_name = parsed_query[2]  # The index name
                    table_name = parsed_query[4]  # The table name
                    columns = parsed_query[5]  # The list of columns

                    self.ddl_manager.create_index(table_name, columns, index_name)

            elif command == "DROP":
                if "TABLE" in parsed_query:
                    # Handle DROP TABLE
                    table_name = parsed_query[2]  # The table name
                    self.ddl_manager.drop_table(table_name)
                elif "INDEX" in parsed_query:
                    # Handle DROP INDEX
                    index_name = parsed_query[2]  # The index name
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
                columns = parsed_query[1]  # Columns to select
                from_part = parsed_query[3]  # The FROM part
                table_name = from_part[0]  # The table name

                if isinstance(from_part[1], list) and from_part[1][0] == "JOIN":
                    join_table = from_part[1][1]  # The table to join
                    on_clause = from_part[1][2]  # The ON clause
                    left_join_col = on_clause[1].split(".")[1]  # The left join column
                    right_join_col = on_clause[3].split(".")[1]  # The right join column

                    where_clause = None
                    for clause in parsed_query[4:]:
                        if clause[0] == "WHERE":
                            where_clause = clause[1:]

                    self.dml_manager.select_with_join(
                        left_table=table_name,
                        right_table=join_table,
                        left_join_col=left_join_col,
                        right_join_col=right_join_col,
                        columns=columns,
                        where=where_clause,
                    )
                else:
                    where_clause = None
                    for clause in parsed_query[4:]:
                        if clause[0] == "WHERE":
                            where_clause = clause[1:]

                    self.dml_manager.select_from_table(
                        table_name=table_name,
                        columns=columns,
                        where=where_clause,
                    )

            else:
                raise Exception("Unsupported SQL command")


if __name__ == "__main__":

    stor_mgr = StorageManager()  # 1) 实例化 StorageManager
    ddl_mgr = DDLManager(stor_mgr)  # 2) 传给 DDLManager
    dml_mgr = DMLManager(stor_mgr)

    # 将这两个对象传入 QueryManager 的构造函数
    qm = QueryManager(ddl_mgr, dml_mgr)

    multi_query = """
    CREATE TABLE Orders (OrderID INT PRIMARY KEY, OrderDate STRING, Amount DOUBLE, UserID INT FOREIGN KEY REFERENCES Users(UserID)) ;
    """

    try:
        parse_results = qm.parse_query(multi_query)
        for i, res in enumerate(parse_results, start=1):
            print(f"语句 {i} 解析成功:")
            print(res)
            print("-" * 50)
    except Exception as e:
        print("解析错误:", e)
