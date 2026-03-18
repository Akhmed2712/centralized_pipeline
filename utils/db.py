import yaml
import pymysql
import os
import yaml
import re

_CONNECTION_MAP = {}

def resolve_env_variables(raw_text):
    pattern = re.compile(r"\$\{(\w+)\}")

    def replacer(match):
        var_name = match.group(1)
        return os.getenv(var_name, "")

    return pattern.sub(replacer, raw_text)

def load_connections(env):
    path = f"config/{env}/connections.yml"

    with open(path) as f:
        raw = f.read()

    resolved = resolve_env_variables(raw)

    config = yaml.safe_load(resolved)

    return config["connections"]

def build_connection_map(connections):
    if isinstance(connections, dict):
        connections = [connections]
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