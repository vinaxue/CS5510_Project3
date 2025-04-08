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

class QueryManager:
    def __init__(self):
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

        self.create_index_stmt = (
            self.CREATE
            + self.INDEX
            + self.identifier("index_name")
            + self.FROM.suppress()  
            ^ (  
                self.CREATE
                + self.INDEX
                + self.identifier("index_name")
                + self.table_name("on_table") 
            )
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
        )


        self.int_type = CaselessKeyword("int")
        self.string_type = CaselessKeyword("string")
        self.column_type = self.int_type | self.string_type


        self.primary_key_clause = Group(self.PRIMARY + self.KEY)
        self.foreign_key_clause = Group(
            self.FOREIGN + self.KEY + self.REFERENCES + self.table_name("ref_table") + Suppress("(") + self.identifier("ref_col") + Suppress(")")
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

        self.drop_table_stmt = (
            self.DROP
            + self.TABLE
            + self.table_name("table_name")
        )
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
                raise Exception(f"Query parsing error in statement '{stmt}': {e}") from e
        return results


if __name__ == "__main__":
    qm = QueryManager()

    multi_query = """
    CREATE TABLE users (id int PRIMARY KEY, name string);
    DROP TABLE users;
    INSERT INTO users (id, name) VALUES (1, 'Alice');
    SELECT id, name FROM users;
    """

    try:
        parse_results = qm.parse_query(multi_query)
        for i, res in enumerate(parse_results, start=1):
            print(f"语句 {i} 解析成功:")
            print(res.dump())
            print("-" * 50)
    except Exception as e:
        print("解析错误:", e)
