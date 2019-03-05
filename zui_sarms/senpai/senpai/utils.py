import json

def load_config(path):
    with open(path, "r") as f:
        r = json.loads(f.read())
    return r
