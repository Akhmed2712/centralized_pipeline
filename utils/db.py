import yaml
import pymysql

_CONNECTION_MAP = {}

def load_connections(env):
    path = f"config/{env}/connections.yaml"

    with open(path) as f:
        config = yaml.safe_load(f)

    return config["connections"]

def build_connection_map(connections):
    return {conn["id"]: conn for conn in connections}

def get_connection(conn_id, env="dev"):
    global _CONNECTION_MAP

    # cache per env
    if env not in _CONNECTION_MAP:
        connections = load_connections(env)
        _CONNECTION_MAP[env] = build_connection_map(connections)

    conn_map = _CONNECTION_MAP[env]

    if conn_id not in conn_map:
        raise ValueError(f"Connection not found: {conn_id} (env={env})")

    conf = conn_map[conn_id]

    if conf["type"] == "mysql":
        return pymysql.connect(
            host=conf["host"],
            port=int(conf.get("port", 3306)),
            user=conf["username"],
            password=conf["password"],
            database=conf["database"],
            charset=conf.get("charset", "utf8mb4"),
            connect_timeout=int(conf.get("connect_timeout", 10)),
        )

    else:
        raise ValueError(f"Unsupported DB type: {conf['type']}")