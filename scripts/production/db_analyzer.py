import psycopg2
from psycopg2.extras import RealDictCursor
import json
from datetime import datetime
import os
from typing import Dict, List


class DatabaseAnalyzer:
    def __init__(self, dbname: str, user: str, password: str, host: str):
        self.conn_params = {
            "dbname": dbname,
            "user": user,
            "password": password,
            "host": host,
        }
        self.output_dir = "db_analysis"
        os.makedirs(self.output_dir, exist_ok=True)

    def connect(self):
        """Establish database connection"""
        return psycopg2.connect(**self.conn_params)

    def get_all_tables(self, conn) -> List[str]:
        """Get list of all tables in the database"""
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
                AND table_type = 'BASE TABLE'
                ORDER BY table_name;
            """)
            return [row[0] for row in cur.fetchall()]

    def get_table_schema(self, conn, table_name: str) -> List[Dict]:
        """Get detailed schema information for a table"""
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 
                    column_name,
                    data_type,
                    character_maximum_length,
                    column_default,
                    is_nullable
                FROM information_schema.columns
                WHERE table_schema = 'public'
                AND table_name = %s
                ORDER BY ordinal_position;
            """,
                (table_name,),
            )

            columns = []
            for row in cur.fetchall():
                column_info = {
                    "name": row[0],
                    "type": row[1],
                    "max_length": row[2],
                    "default": row[3],
                    "nullable": row[4],
                }
                columns.append(column_info)

            return columns

    def get_table_constraints(self, conn, table_name: str) -> List[Dict]:
        """Get constraints information for a table"""
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 
                    c.conname as constraint_name,
                    c.contype as constraint_type,
                    pg_get_constraintdef(c.oid) as definition
                FROM pg_constraint c
                JOIN pg_namespace n ON n.oid = c.connamespace
                JOIN pg_class cl ON cl.oid = c.conrelid
                WHERE n.nspname = 'public'
                AND cl.relname = %s;
            """,
                (table_name,),
            )

            constraints = []
            for row in cur.fetchall():
                constraint_info = {"name": row[0], "type": row[1], "definition": row[2]}
                constraints.append(constraint_info)

            return constraints

    def get_table_indexes(self, conn, table_name: str) -> List[Dict]:
        """Get indexes information for a table"""
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    i.relname as index_name,
                    a.attname as column_name,
                    ix.indisunique as is_unique,
                    ix.indisprimary as is_primary
                FROM pg_class t
                JOIN pg_index ix ON t.oid = ix.indrelid
                JOIN pg_class i ON ix.indexrelid = i.oid
                JOIN pg_attribute a ON a.attrelid = t.oid
                WHERE t.relname = %s
                AND a.attnum = ANY(ix.indkey)
                AND t.relkind = 'r'
                ORDER BY i.relname;
            """,
                (table_name,),
            )

            indexes = []
            for row in cur.fetchall():
                index_info = {
                    "name": row[0],
                    "column": row[1],
                    "unique": row[2],
                    "primary": row[3],
                }
                indexes.append(index_info)

            return indexes

    def get_table_stats(self, conn, table_name: str) -> Dict:
        """Get basic statistics about the table"""
        with conn.cursor() as cur:
            # Get row count
            cur.execute(f"SELECT COUNT(*) FROM {table_name};")
            row_count = cur.fetchone()[0]

            # Get size information
            cur.execute(
                """
                SELECT 
                    pg_size_pretty(pg_total_relation_size(%s)) as total_size,
                    pg_size_pretty(pg_table_size(%s)) as table_size,
                    pg_size_pretty(pg_indexes_size(%s)) as index_size
                """,
                (table_name, table_name, table_name),
            )
            sizes = cur.fetchone()

            return {
                "row_count": row_count,
                "total_size": sizes[0],
                "table_size": sizes[1],
                "index_size": sizes[2],
            }

    def get_sample_data(self, conn, table_name: str, limit: int = 5) -> List[Dict]:
        """Get sample rows from the table"""
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(f"SELECT * FROM {table_name} LIMIT {limit};")
            return [dict(row) for row in cur.fetchall()]

    def analyze_database(self, sample_size: int = 5):
        """Perform complete database analysis"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(self.output_dir, f"db_analysis_{timestamp}.json")

        analysis = {
            "database": self.conn_params["dbname"],
            "timestamp": timestamp,
            "tables": {},
        }

        try:
            conn = self.connect()
            tables = self.get_all_tables(conn)

            print(f"Analyzing {len(tables)} tables...")

            for table_name in tables:
                print(f"Processing table: {table_name}")

                table_info = {
                    "schema": self.get_table_schema(conn, table_name),
                    "constraints": self.get_table_constraints(conn, table_name),
                    "indexes": self.get_table_indexes(conn, table_name),
                    "statistics": self.get_table_stats(conn, table_name),
                    "sample_data": self.get_sample_data(conn, table_name, sample_size),
                }

                analysis["tables"][table_name] = table_info

            # Write analysis to file
            with open(output_file, "w") as f:
                json.dump(analysis, f, indent=2, default=str)

            print(f"\nAnalysis complete! Results written to: {output_file}")

            # Print summary to console
            print("\nDatabase Summary:")
            print(f"Total tables: {len(tables)}")
            for table_name, info in analysis["tables"].items():
                stats = info["statistics"]
                print(f"\n{table_name}:")
                print(f"  Rows: {stats['row_count']}")
                print(f"  Size: {stats['total_size']}")

        finally:
            if "conn" in locals():
                conn.close()


def main():
    from config import DB_NAME, DB_USER, DB_PASSWORD, DB_HOST

    analyzer = DatabaseAnalyzer(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST
    )

    analyzer.analyze_database(sample_size=5)


if __name__ == "__main__":
    main()
