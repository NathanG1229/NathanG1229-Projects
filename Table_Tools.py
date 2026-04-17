import sqlite3


def quote_identifier(name):
    return '"' + name.replace('"', '""') + '"'

def combine_all_tables(db_path):
    TARGET_TABLE = "all"

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
        AND name NOT LIKE 'sqlite_%'
        AND name != ?
        ORDER BY name;
        """,
        (TARGET_TABLE,),
    )
    source_tables = [row[0] for row in cursor.fetchall()]

    if not source_tables:
        print("No source tables found to combine.")
        conn.close()
        raise SystemExit()

    column_names = []
    column_defs = []
    seen_columns = set()

    for table_name in source_tables:
        table_info = cursor.execute(
            f"PRAGMA table_info({quote_identifier(table_name)});"
        ).fetchall()

        for _, column_name, column_type, _, _, _ in table_info:
            if column_name not in seen_columns:
                seen_columns.add(column_name)
                column_names.append(column_name)
                column_defs.append(
                    f"{quote_identifier(column_name)} {column_type or 'TEXT'}"
                )

    cursor.execute(f"DROP TABLE IF EXISTS {quote_identifier(TARGET_TABLE)};")
    cursor.execute(
        f"CREATE TABLE {quote_identifier(TARGET_TABLE)} ({', '.join(column_defs)});"
    )

    insert_columns = ", ".join(quote_identifier(column) for column in column_names)

    for table_name in source_tables:
        table_columns = {
            row[1]
            for row in cursor.execute(
                f"PRAGMA table_info({quote_identifier(table_name)});"
            ).fetchall()
        }

        select_columns = ", ".join(
            quote_identifier(column)
            if column in table_columns
            else f"NULL AS {quote_identifier(column)}"
            for column in column_names
        )

        cursor.execute(
            f"INSERT INTO {quote_identifier(TARGET_TABLE)} ({insert_columns}) "
            f"SELECT {select_columns} FROM {quote_identifier(table_name)};"
        )

        row_count = cursor.execute(
            f"SELECT COUNT(*) FROM {quote_identifier(table_name)};"
        ).fetchone()[0]
        print(f"Added {row_count} rows from {table_name}")

    conn.commit()

    total_rows = cursor.execute(
        f"SELECT COUNT(*) FROM {quote_identifier(TARGET_TABLE)};"
    ).fetchone()[0]

    print(f"Combined {len(source_tables)} tables into {TARGET_TABLE} with {total_rows} rows.")

    conn.close()

def list_tables(db_path):

    db_path = db_path

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    query = """
    SELECT name
    FROM sqlite_master
    WHERE type='table' AND name NOT LIKE 'sqlite_%'
    ORDER BY name;
    """

    cursor.execute(query)
    tables = [row[0] for row in cursor.fetchall()]

    for table_name in tables:
        print(table_name)

    conn.close()

def rename_tables(db_path):

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT name
        FROM sqlite_master
        WHERE type='table' AND name NOT LIKE 'sqlite_%'
        ORDER BY name;
    """)

    tables = [table[0] for table in cursor.fetchall()]
    sector_tables = [table_name for table_name in tables if table_name.startswith("sector")]

    if not sector_tables:
        print("No sector-prefixed tables found. No changes made.")
    else:
        for table_name in tables:
            if table_name.startswith("sector"):
                new_name = table_name[len("sector"):].lstrip("_")

                if new_name and new_name != table_name:
                    cursor.execute(
                        f"ALTER TABLE {quote_identifier(table_name)} RENAME TO {quote_identifier(new_name)};"
                    )
                    print(f"Renamed: {table_name} -> {new_name}")
                else:
                    print(f"Skipped rename for: {table_name}")
            else:
                cursor.execute(f"DROP TABLE IF EXISTS {quote_identifier(table_name)};")
                print(f"Deleted: {table_name}")

        conn.commit()
        print("Done updating company_facts.db.")

    conn.close()