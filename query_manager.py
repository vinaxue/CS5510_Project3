from pyparsing import (
    CaselessKeyword, Word, alphas, alphanums, delimitedList, Group, Optional, 
    Forward, oneOf, quotedString, removeQuotes, Suppress, nums, Combine, OneOrMore
)

class QueryManager:
    def __init__(self):
        self.identifier = Word(alphas, alphanums + "_").setName("identifier")
        
        integer = Word(nums)
        self.numeric_literal = Combine(Optional(oneOf("+ -")) + integer)
        
        self.string_literal = quotedString.setParseAction(removeQuotes)
        
        self.constant = self.numeric_literal | self.string_literal
        
        (self.SELECT, self.FROM, self.WHERE, self.GROUP, self.BY, self.ORDER, 
         self.INSERT, self.INTO, self.VALUES, self.JOIN, self.ON) = map(
            CaselessKeyword, 
            "SELECT FROM WHERE GROUP BY ORDER INSERT INTO VALUES JOIN ON".split()
        )
        
        self.column_name = self.identifier
        self.column_list = Group(delimitedList(self.column_name))
        
        self.table_name = self.identifier
        self.join_condition = Group(self.ON + self.identifier + "=" + self.identifier)
        self.join_clause = Group(self.JOIN + self.table_name + self.join_condition)
        self.table_with_joins = Group(self.table_name + Optional(OneOrMore(self.join_clause)))
        
        self.select_stmt = Forward()
        self.where_condition = Group(self.WHERE + self.identifier + oneOf("= > < >= <=") + (self.constant | self.identifier))
        self.group_by_clause = Group(self.GROUP + self.BY + self.column_list)
        self.order_by_clause = Group(self.ORDER + self.BY + self.column_list)
        self.select_stmt << (
            self.SELECT 
            + (Group(delimitedList(self.identifier)) | "*")
            + self.FROM + self.table_with_joins
            + Optional(self.where_condition)
            + Optional(self.group_by_clause)
            + Optional(self.order_by_clause)
        )
        
        self.insert_stmt = (
            self.INSERT + self.INTO + self.table_name("table")
            + Optional(Suppress("(") + Group(delimitedList(self.identifier))("columns") + Suppress(")"))
            + self.VALUES
            + Suppress("(") + Group(delimitedList(self.constant))("values") + Suppress(")")
        )
        
        self.sql_stmt = self.select_stmt("select") | self.insert_stmt("insert")
    
    def parse_query(self, query: str):
        try:
            result = self.sql_stmt.parseString(query)
            return result
        except Exception as e:
            raise Exception(f"Query parsing error: {e}") from e

if __name__ == "__main__":
    qm = QueryManager()
    
    examples = [
        """SELECT id, name FROM users JOIN orders ON users.id = orders.user_id WHERE age >= 18 GROUP BY country ORDER BY name""",
        """INSERT INTO users (id, name, age) VALUES (1, "Alice", 30)"""
    ]
    
    for sql in examples:
        try:
            result = qm.parse_query(sql)
            print("解析成功:")
            print(result.dump())
            print("-" * 50)
        except Exception as e:
            print("解析错误:", e)