import json
import logging
import os

def init_storage():
    if not os.path.exists("./storage"):
        os.mknod("./storage")
    initial_state = {}
    dump_storage(initial_state)

def load_storage():
    with open("./storage", "r") as st:
        data = json.load(st)
    return data

def dump_storage(data):
    with open("./storage", "w") as st:
        json.dump(data, st)
        st.flush()

def update(key, value):
    data = load_storage()
    if key in data:
        data[key] = value
        dump_storage(data)
        return True
    return False

def create(key, value):
    data = load_storage()
    data[key] = value
    dump_storage(data)
    return True

def delete(key, value):
    data = load_storage()
    if key in data:
        del data[key]
        dump_storage(data)
        return True
    return False

def read(key):
    data = load_storage()
    logging.info(f"storage: {data}")
    if key in data:
        return data[key]
    return None

def do_change(change):
    if change["opcode"] == "UPDATE":
        update(key=change["key"], value=change["value"])
    if change["opcode"] == "CREATE":
        create(key=change["key"], value=change["value"])
    if change["opcode"] == "DELETE":
        delete(key=change["key"])