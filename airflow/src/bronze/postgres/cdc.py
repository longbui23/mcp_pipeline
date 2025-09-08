import psycopg2
import psycopg2.extras
import json
import time

# -----------------------------
# Connection params
# -----------------------------
PG_HOST = "localhost"
PG_PORT = 5432
PG_DB = "your_db"
PG_USER = "your_user"
PG_PASSWORD = "your_password"

REPLICATION_SLOT = "cdc_slot"
PUBLICATION_NAME = "cdc_pub"

# -----------------------------
# CDC Logic
# -----------------------------
def create_replication_slot(conn):
    """Create logical replication slot if it does not exist"""
    with conn.cursor() as cur:
        try:
            cur.execute(
                f"SELECT * FROM pg_create_logical_replication_slot('{REPLICATION_SLOT}', 'wal2json');"
            )
            print(f"Created replication slot: {REPLICATION_SLOT}")
        except psycopg2.errors.DuplicateObject:
            print(f"Replication slot {REPLICATION_SLOT} already exists")
        conn.commit()


def create_publication(conn, table: str):
    """Create publication for a table"""
    with conn.cursor() as cur:
        cur.execute(
            f"CREATE PUBLICATION {PUBLICATION_NAME} FOR TABLE {table};"
        )
        conn.commit()
        print(f"Created publication {PUBLICATION_NAME} for table {table}")


def start_cdc():
    """Connect via replication and stream changes"""
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
        connection_factory=psycopg2.extras.LogicalReplicationConnection,
    )

    cur = conn.cursor()
    create_replication_slot(conn)
    # Assuming publication already exists; otherwise call create_publication(conn, "your_table")

    print("Starting CDC streaming...")
    cur.start_replication(slot_name=REPLICATION_SLOT, options={"pretty-print": 1}, decode=True)

    def consume(msg):
        data = json.loads(msg.payload)
        # Print each change
        for change in data.get("changes", []):
            print(json.dumps(change, indent=2))
        msg.cursor.send_feedback(flush_lsn=msg.data_start)

    try:
        cur.consume_stream(consume)
    except KeyboardInterrupt:
        print("Stopping CDC...")
        cur.close()
        conn.close()


if __name__ == "__main__":
    start_cdc()
