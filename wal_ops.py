import json
import os

def init_wal():
    if not os.path.exists("./WAL"):
        os.mknod("./WAL")
    initial_state = []
    dump_wal(initial_state)

def load_wal():
    with open("./WAL", "r") as wal:
        data = json.load(wal)
    return data

def dump_wal(data):
    with open("./WAL", "w") as wal:
        json.dump(data, wal)
        wal.flush()

def write_to_wal(change):
    if "opcode" not in change or "key" not in change or "value" not in change:
        raise ValueError("Invalid WAL format")
    data = load_wal()
    length = len(data)
    change["idx"] = length + 1
    data.append(change)
    dump_wal(data)

def read_from_wal(change_no):
    if change_no < 1:
        return None
    data = load_wal()
    change = data[change_no - 1]
    return change

def last_from_wal():
    data = load_wal()
    x = []
    if data:
        change = data[-1]
        return change["idx"]
    return 0