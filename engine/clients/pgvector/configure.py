import psycopg2
from engine.base_client.configure import BaseConfigurator
from engine.clients.pgvector.config import *


class PGVectorConfigurator(BaseConfigurator):
    conn = None
    cur = None

    def __init__(self, host, collection_params: dict, connection_params: dict):
        super().__init__(host, collection_params, connection_params)
        database, host, port, user, password = process_connection_params(connection_params, host)
        self.database = database
        self.host = host
        self.port = port
        self.user = user
        self.password = password

    def init_client(self):
        self.conn = psycopg2.connect(database=self.database, user=self.user, password=self.password, host=self.host,
                                     port=self.port)
        self.cur = self.conn.cursor()

    def release_client(self):
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()

    def clean(self):
        try:
            self.init_client()
            self.cur.execute(f"DROP TABLE {PGVECTOR_INDEX};")
            self.conn.commit()
            print(f"PGVector {PGVECTOR_INDEX} dropped successfully.")
        except psycopg2.Error as e:
            print(f"Error dropping the table {PGVECTOR_INDEX}: {e}")
        finally:
            # Commit the transaction
            self.release_client()

    def recreate(self, distance, vector_size, collection_params, connection_params, extra_columns_name,
                 extra_columns_type):
        try:
            self.init_client()
        except Exception as e:
            print(f"Failed to init PGVector client: {e}")

        # Base command
        command = f"CREATE TABLE {PGVECTOR_INDEX} (id bigserial PRIMARY KEY, vector vector({vector_size}))"
        # Add extra columns if provided
        if extra_columns_name and extra_columns_type and len(extra_columns_name) == len(extra_columns_type) and len(
                extra_columns_name) > 0:
            columns = ", ".join([f"{name} {H5_COLUMN_TYPES_MAPPING.get(_type, _type)}" for name, _type in zip(extra_columns_name, extra_columns_type)])
            command = command[:-1] + f", {columns});"

        extensions = []
        try:
            self.cur.execute("SELECT extname FROM pg_extension;")
            extensions = [row[0] for row in self.cur.fetchall()]
        except Exception as e:
            print(f"Failed to get PGVector extensions: {e}")
        # create extension
        if 'vector' in extensions or 'vectors' in extensions:
            try:
                self.cur.execute("ALTER EXTENSION vector UPDATE;")
                self.conn.commit()
            except Exception as e:
                self.conn.rollback()
                print(f"Failed to update the 'vector' extension:{e}")
            try:
                self.cur.execute("ALTER EXTENSION vectors UPDATE;")
                self.conn.commit()
            except Exception as e:
                self.conn.rollback()
                print(f"Failed to update the 'vectors' extension:{e}")
        else:
            try:
                self.cur.execute("CREATE EXTENSION vector;")
                self.conn.commit()
            except Exception as e:
                self.conn.rollback()
                print(f"Failed to create the 'vector' extension:{e}")
            try:
                self.cur.execute("CREATE EXTENSION vectors;")
                self.conn.commit()
            except Exception as e:
                self.conn.rollback()
                print(f"Failed to create the 'vectors' extension:{e}")
        # create table
        try:
            print(f"try create table: {command}")
            self.cur.execute(command)
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            print(f"Exception happened when recreate table[{PGVECTOR_INDEX}], exp:{e}")
        finally:
            self.release_client()


