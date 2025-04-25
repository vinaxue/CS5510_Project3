from ddl_manager import DDLManager
from dml_manager import DMLManager
from query_manager import QueryManager
from storage_manager import StorageManager
from utils import INT, track_time


storage_manager = StorageManager(
    db_file="./data/sample_data.pkl", index_file="./data/sample_index.pkl"
)
ddl_manager = DDLManager(storage_manager)
dml_manager = DMLManager(storage_manager)


@track_time
def load_data(name, data):
    try:
        ddl_manager.create_table(
            name,
            [("id", INT), ("value", INT)],
            primary_key="id",
        )
    except Exception as e:
        print(f"Error creating table {name}: {e}. Dropping table and retrying.")
        ddl_manager.drop_table(name)
        ddl_manager.create_table(
            name,
            [("id", INT), ("value", INT)],
            primary_key="id",
        )

    # Build once before starting
    table_data = storage_manager.db["DATA"][name]
    indexes = storage_manager.index.get(name, {})
    col_names = list(storage_manager.db["COLUMNS"][name].keys())
    pk_col = storage_manager.db["TABLES"][name].get("primary_key")

    for i, (val0, val1) in enumerate(data):
        row = [val0, val1]
        # Append directly
        table_data.append(row)
        row_id = len(table_data) - 1

        # Update in-memory indexes
        for col_name, index in indexes.items():
            col_index = col_names.index(col_name)
            value = row[col_index]
            tree = index["tree"]
            if value not in tree:
                tree[value] = []
            tree[value].append(row_id)

        if (i + 1) % 10000 == 0:
            print((i + 1))

    # Save once at the end
    storage_manager.save_db()
    storage_manager.save_index()


rel_i_i_1000 = []
for i in range(1000):
    rel_i_i_1000.append([i, i])
#load_data("rel_i_i_1000", rel_i_i_1000)


rel_i_1_1000 = []
for i in range(1000):
    rel_i_1_1000.append([i, 1])
#load_data("rel_i_1_1000", rel_i_1_1000)


rel_i_i_100000 = []
for i in range(100000):
    rel_i_i_100000.append([i, i])
#load_data("rel_i_i_100000", rel_i_i_100000)

rel_i_1_100000 = []
for i in range(100000):
    rel_i_1_100000.append([i, 1])
#load_data("rel_i_1_100000", rel_i_1_100000)

rel_i_i_1000000 = []
for i in range(1000000):
    rel_i_i_1000000.append([i, i])
#load_data("rel_i_i_1000000", rel_i_i_1000000)

rel_i_1_1000000 = []
for i in range(1000000):
    rel_i_1_1000000.append([i,1])
#load_data("rel_i_1_1000000", rel_i_1_1000000) 

# db = storage_manager.load_db()
# print("rel_i_i_1000", len(db["DATA"]["rel_i_i_1000"]))
# print("rel_i_1_1000", len(db["DATA"]["rel_i_1_1000"]))
# print("rel_i_i_100000", len(db["DATA"]["rel_i_i_100000"]))
# print("rel_i_1_100000", len(db["DATA"]["rel_i_1_100000"]))


# @track_time
# def select_data(name, where):
#     result = dml_manager.select(name, where=where)
#     return result


# print("rel_i_i_1000 [835]", select_data("rel_i_i_1000", ["value", "=", 835]))
# print("rel_i_1_1000 [835]", select_data("rel_i_1_1000", ["id", "=", 835]))
# print("rel_i_i_100000 [835]", select_data("rel_i_i_100000", ["value", "=", 2835]))
# print("rel_i_1_100000 [835]", select_data("rel_i_1_100000", ["id", "=", 2835]))
# print(
#     "rel_i_i_100000 [835, 3004]",
#     select_data(
#         "rel_i_i_100000",
#         {"op": "OR", "left": ["value", "=", 835], "right": ["id", "=", 3004]},
#     ),
# )

query = "SELECT id FROM rel_i_1_100000 WHERE id < 300 GROUP BY id;"
query_manager = QueryManager(storage_manager, ddl_manager, dml_manager)

res, runtime = query_manager.execute_query(query)
print(len(res))
ddl_manager.create_index("rel_i_1_100000", "id")
print()
res = dml_manager.select_join_with_index(
    left_table="rel_i_i_100000",
    right_table="rel_i_1_1000",
    left_join_col="id",
    right_join_col="id",
)
print(len(res))
