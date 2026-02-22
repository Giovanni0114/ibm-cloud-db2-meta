import os
from pathlib import Path

import ibm_db


class DatabaseConfig:
    def __init__(self) -> None:
        self.host = os.getenv("DB2_HOST")
        self.database = os.getenv("DB2_DATABASE")
        self.uid = os.getenv("DB2_UID")
        pwd = os.getenv("DB2_PWD")
        self.port = os.getenv("DB2_PORT")
        self.ssl_cert = os.getenv("DB2_SSL_CERT")

        if not Path(self.ssl_cert).exists():
            raise FileNotFoundError(f"SSL certificate not found: {self.ssl_cert}")

        self.dsn = (
            f"DATABASE={self.database};"
            f"HOSTNAME={self.host};"
            f"PORT={self.port};"
            f"PROTOCOL=TCPIP;"
            f"UID={self.uid};"
            f"PWD={pwd};"
            "SECURITY=SSL;"
            f"SSLServerCertificate={self.ssl_cert};"
        )

    def get_dns(self) -> str:
        return self.dsn

    def print(self):
        print("Database connection settings:")
        print(f"  Host: {self.host}")
        print(f"  Port: {self.port}")
        print(f"  Database: {self.database}")
        print(f"  User: {self.uid}")
        print(f"  SSL Path: {self.ssl_cert}")


class DB2Connections:
    def _create_table_if_not_exists(self) -> None:
        sql = """
            CREATE TABLE IF NOT EXISTS metadata (
                key   VARCHAR(80)  NOT NULL PRIMARY KEY,
                value VARCHAR(200) NOT NULL
            )
        """
        try:
            ibm_db.exec_immediate(self.conn, sql)
            print("[OK] Table 'metadata' is ready.\n")
        except Exception as e:
            if "-601" in str(e):
                print("[OK] Table 'metadata' already exists.\n")
            else:
                raise

    def __init__(self, dsn_config: str) -> None:
        self.dsn = dsn_config

    def __enter__(self):
        try:
            print("[OK] Connecting to DB2")
            self.conn = ibm_db.connect(self.dsn, "", "")
            print("[OK] Connected to Db2.\n")
            self._create_table_if_not_exists()
            return self.conn
        except Exception as e:
            print(f"[ERROR] Could not connect: {e}")
            raise

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        ibm_db.close(self.conn)
        print("\n[OK] Connection closed.")


# ─── CRUD operations ──────────────────────────────────────────────────────────


def set_key(conn: ibm_db.IBM_DBConnection, key: str, value: str) -> None:
    """Insert or update a key/value pair (UPSERT via MERGE)."""
    if len(key) > 80:
        print(f"[ERROR] Key too long (max 80 chars): '{key}'")
        return
    if len(value) > 200:
        print(f"[ERROR] Value too long (max 200 chars): '{value}'")
        return

    sql = """
        MERGE INTO metadata AS tgt
        USING (VALUES (?, ?)) AS src (key, value)
        ON tgt.key = src.key
        WHEN MATCHED     THEN UPDATE SET tgt.value = src.value
        WHEN NOT MATCHED THEN INSERT (key, value) VALUES (src.key, src.value)
    """
    stmt = ibm_db.prepare(conn, sql)
    ibm_db.bind_param(stmt, 1, key)
    ibm_db.bind_param(stmt, 2, value)
    ibm_db.execute(stmt)
    print(f"[OK] Set  '{key}' = '{value}'")


def get_key(conn: ibm_db.IBM_DBConnection, key: str) -> str | None:
    """Retrieve a single value by key. Returns None if not found."""
    sql = "SELECT value FROM metadata WHERE key = ?"
    stmt = ibm_db.prepare(conn, sql)
    ibm_db.bind_param(stmt, 1, key)
    ibm_db.execute(stmt)
    row = ibm_db.fetch_assoc(stmt)
    if row:
        return row["VALUE"]
    return None


def delete_key(conn: ibm_db.IBM_DBConnection, key: str) -> None:
    """Delete a key/value pair."""
    sql = "DELETE FROM metadata WHERE key = ?"
    stmt = ibm_db.prepare(conn, sql)
    ibm_db.bind_param(stmt, 1, key)
    ibm_db.execute(stmt)
    print(f"[OK] Deleted key '{key}'")


def list_all(conn: ibm_db.IBM_DBConnection) -> list[tuple[str, str]]:
    """Return all key/value pairs sorted by key."""
    sql = "SELECT key, value FROM metadata ORDER BY key"
    stmt = ibm_db.exec_immediate(conn, sql)
    rows = []
    row = ibm_db.fetch_assoc(stmt)
    while row:
        rows.append((row["KEY"], row["VALUE"]))
        row = ibm_db.fetch_assoc(stmt)
    return rows


def search_keys(conn: ibm_db.IBM_DBConnection, pattern: str) -> list[tuple[str, str]]:
    """Search keys using a LIKE pattern, e.g. 'app.%'."""
    sql = "SELECT key, value FROM metadata WHERE key LIKE ? ORDER BY key"
    stmt = ibm_db.prepare(conn, sql)
    ibm_db.bind_param(stmt, 1, pattern)
    ibm_db.execute(stmt)
    rows = []
    row = ibm_db.fetch_assoc(stmt)
    while row:
        rows.append((row["KEY"], row["VALUE"]))
        row = ibm_db.fetch_assoc(stmt)
    return rows


# ─── Display helpers ──────────────────────────────────────────────────────────


def print_table(rows: list[tuple[str, str]]) -> None:
    if not rows:
        print("  (no records found)")
        return
    key_w = max(len(r[0]) for r in rows)
    key_w = max(key_w, 4)
    val_w = max(len(r[1]) for r in rows)
    val_w = max(val_w, 5)
    sep = f"  +{'-' * (key_w + 2)}+{'-' * (val_w + 2)}+"
    print(sep)
    print(f"  | {'KEY':<{key_w}} | {'VALUE':<{val_w}} |")
    print(sep)
    for k, v in rows:
        print(f"  | {k:<{key_w}} | {v:<{val_w}} |")
    print(sep)
    print(f"  {len(rows)} row(s)\n")


def print_help() -> None:
    print("""
Commands:
  set  <key> <value>   Insert or update a key/value pair
  get  <key>           Retrieve value for a key
  del  <key>           Delete a key
  list                 List all key/value pairs
  find <pattern>       Search keys by LIKE pattern (e.g.  app.%)
  help                 Show this help
  exit                 Quit
""")


def repl(conn: ibm_db.IBM_DBConnection) -> None:
    print("Db2 Metadata Manager  |  type 'help' for commands\n")
    while True:
        try:
            line = input("db2-meta> ").strip()
        except (EOFError, KeyboardInterrupt):
            break

        if not line:
            continue

        parts = line.split(maxsplit=2)
        cmd = parts[0].lower()

        match cmd:
            case "exit" | "quit" | "q":
                break

            case "help":
                print_help()

            case "set":
                if len(parts) < 3:
                    print("[ERROR] Usage: set <key> <value>")
                    continue

                set_key(conn, parts[1], parts[2])

            case "get":
                if len(parts) < 2:
                    print("[ERROR] Usage: get <key>")
                    continue

                val = get_key(conn, parts[1])

                if val is None:
                    print(f"  (key '{parts[1]}' not found)")
                    continue

                print(f"  {parts[1]} = {val}")

            case "del":
                if len(parts) < 2:
                    print("[ERROR] Usage: del <key>")
                    continue

                delete_key(conn, parts[1])

            case "list":
                rows = list_all(conn)
                print_table(rows)

            case "find":
                if len(parts) < 2:
                    print("[ERROR] Usage: find <pattern>  e.g.  find app.%")
                    continue

                rows = search_keys(conn, parts[1])
                print_table(rows)

            case _:
                print(f"[ERROR] Unknown command '{cmd}'. Type 'help'.")


def main():
    config = DatabaseConfig()
    config.print()

    with DB2Connections(config.get_dns()) as conn:
        repl(conn)


if __name__ == "__main__":
    main()
