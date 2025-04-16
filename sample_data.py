from ddl_manager import DDLManager
from dml_manager import DMLManager
from storage_manager import StorageManager
from utils import INT, track_time


storage_manager = StorageManager(
    db_file="./data/sample_data.pkl", index_file="./data/sample_index.pkl"
)
ddl_manager = DDLManager(storage_manager)
dml_manager = DMLManager(storage_manager)


@track_time
def load_data(name, data):
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
# load_data("rel_i_i_1000", rel_i_i_1000)


rel_i_1_1000 = []
for i in range(1000):
    rel_i_1_1000.append([i, 1])
# load_data("rel_i_1_1000", rel_i_1_1000)


rel_i_i_100000 = []
for i in range(100000):
    rel_i_i_100000.append([i, i])
# load_data("rel_i_i_100000", rel_i_i_100000)

rel_i_1_100000 = []
for i in range(100000):
    rel_i_1_100000.append([i, 1])
# load_data("rel_i_1_100000", rel_i_1_100000)


db = storage_manager.load_db()
print("rel_i_i_1000", len(db["DATA"]["rel_i_i_1000"]))
print("rel_i_1_1000", len(db["DATA"]["rel_i_1_1000"]))
print("rel_i_i_100000", len(db["DATA"]["rel_i_i_100000"]))
print("rel_i_1_100000", len(db["DATA"]["rel_i_1_100000"]))


@track_time
def select_data(name, where):
    result = dml_manager.select(name, where=where)
    return result


print("rel_i_i_1000 [835]", select_data("rel_i_i_1000", ["value", "=", 835]))
print("rel_i_1_1000 [835]", select_data("rel_i_1_1000", ["id", "=", 835]))
print("rel_i_i_100000 [835]", select_data("rel_i_i_100000", ["value", "=", 2835]))
print("rel_i_1_100000 [835]", select_data("rel_i_1_100000", ["id", "=", 2835]))
print(
    "rel_i_i_100000 [835, 3004]",
    select_data(
        "rel_i_i_100000",
        {"op": "OR", "left": ["value", "=", 835], "right": ["id", "=", 3004]},
    ),
)
