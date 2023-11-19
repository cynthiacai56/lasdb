import time
from psycopg2 import connect, Error, extras


class Postgres:
    def __init__(self, db_conf, name):
        self.db_conf = db_conf
        self.connection = None
        self.cursor = None

        self.meta_table = "pc_metadata_" + name
        self.point_table = "pc_record_" + name
        self.btree_index = "btree_" + name

    def load(self, metadata, file="./cache/pc_record.csv"):
        start_time = time.time()
        self.connect()

        self.create_table()
        self.insert_metadata(metadata)

        if isinstance(file, str):
            self.copy_points(file)
        elif isinstance(file, list):
            for f in file:
                self.copy_points(f)

        load_time = time.time()
        print("Loading time:", round(load_time - start_time, 2))

        self.create_btree_index()
        self.disconnect()
        print("Close time:", round(time.time() - load_time, 2))

    def connect(self):
        try:
            self.connection = connect(
                dbname=self.db_conf["dbname"],
                user=self.db_conf["user"],
                password=self.db_conf["password"],
                host=self.db_conf["host"],
                port=self.db_conf["port"]
            )
            self.cursor = self.connection.cursor()
        except Error as e:
            print("Error: Unable to connect to the database.")
            print(e)

    def disconnect(self):
        if self.connection:
            self.cursor.close()
            self.connection.close()
            self.connection = None
            self.cursor = None

    def create_table(self, name="default"):
        if not self.connection:
            print("Error: Database connection is not established.")
            return

        create_table_sql = f"""
            CREATE EXTENSION IF NOT EXISTS postgis;
            CREATE TABLE IF NOT EXISTS {self.meta_table} (
                name TEXT,
                srid INT,
                point_count BIGINT,
                ratio DOUBLE PRECISION,
                scales DOUBLE PRECISION[],
                offsets DOUBLE PRECISION[],
                bbox DOUBLE PRECISION[]
            );        
            CREATE TABLE IF NOT EXISTS {self.point_table} (
                sfc_head INT,
                sfc_tail INT[],
                z DOUBLE PRECISION[]
            );
            """
        try:
            self.cursor.execute(create_table_sql)
            self.connection.commit()
        except Error as e:
            print("Error: Unable to create table")
            print(e)
            self.connection.rollback()

    def execute_sql(self, sql, data=None):
        if not self.connection:
            print("Error: Database connection is not established.")
            return
        try:
            if data:
                self.cursor.execute(sql, data)
            else:
                self.cursor.execute(sql)
            self.connection.commit()
        except Error as e:
            print(f"Error: Unable to execute query: {sql}")
            print(e)
            self.connection.rollback()

    def insert_metadata(self, data):
        if not self.connection:
            print("Error: Database connection is not established.")
            return

        try:
            self.cursor.execute(f"INSERT INTO {self.meta_table} VALUES (%s, %s, %s, %s, %s, %s, %s);", data)
            self.connection.commit()
        except Error as e:
            print(f"Error: Unable to insert metadata.")
            print(e)
            self.connection.rollback()

    def copy_points(self, file):
        if not self.connection:
            print("Error: Database connection is not established.")
            return

        with open(file, 'r') as f:
            try:
                self.cursor.copy_expert(sql=f"COPY {self.point_table} FROM stdin WITH CSV HEADER", file=f)
                self.connection.commit()
            except Error as e:
                print("Error: Unable to copy the data.")
                print(e)
                self.connection.rollback()

    def execute_query(self, data, name="default"):
        sql = f"SELECT * FROM {self.point_table} WHERE sfc_head IN %(data)s"
        self.cursor.execute(sql, {'data': tuple(data)})
        results = self.cursor.fetchall()

        for row in results:
            print(row)


    def create_btree_index(self, name="default"):
        sql = f"CREATE INDEX {self.btree_index} ON {self.point_table} USING btree (sfc_head)"
        try:
            self.cursor.execute(sql)
            self.connection.commit()
        except Error as e:
            print(f"Error: Unable to execute query: {sql}")
            print(e)
            self.connection.rollback()



