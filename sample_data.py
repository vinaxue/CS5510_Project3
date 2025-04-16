from ddl_manager import DDLManager
from dml_manager import DMLManager
from storage_manager import StorageManager
from utils import INT, track_time


storage_manager = StorageManager(
    db_file="./data/sample_data.dat", index_file="./data/sample_index.db"
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
    for i, (value) in enumerate(data):
        dml_manager.insert(name, [value[0], value[1]])
        if (i + 1) % 1000 == 0:
            print(f"Inserted {i+1} rows into {name}")


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
load_data("rel_i_i_100000", rel_i_i_100000)

rel_i_1_100000 = []
for i in range(100000):
    rel_i_1_100000.append([i, 1])
# load_data("rel_i_1_100000", rel_i_1_100000)
