from psycopg2 import connect, Error, extras


class PgDatabase:
    def __init__(self, dbname, user, password, host, port):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.connection = None
        self.cursor = None

    def connect(self):
        try:
            self.connection = connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
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

        table_name_meta = "pc_metadata_" + name
        table_name_point = "pc_record_" + name
        create_table_sql = f"""
            CREATE EXTENSION IF NOT EXISTS postgis;
            CREATE TABLE IF NOT EXISTS {table_name_meta} (
                name TEXT,
                srid INT,
                point_count BIGINT,
                head_length INT,
                tail_length INT,
                bbox DOUBLE PRECISION[]
            );        
            CREATE TABLE IF NOT EXISTS {table_name_point} (
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

    def execute_copy(self, filename, name="default"):
        if not self.connection:
            print("Error: Database connection is not established.")
            return

        table_name = "pc_record_" + name
        with open(filename, 'r') as f:
            try:
                self.cursor.copy_expert(sql=f"COPY {table_name} FROM stdin WITH CSV HEADER", file=f)
                self.connection.commit()
                #print("Data copied successfully.")
            except Error as e:
                print("Error: Unable to copy the data.")
                print(e)
                self.connection.rollback()

    def execute_query(self, data, name="default"):
        table_name = "pc_record_" + name
        query = f"SELECT * FROM {table_name} WHERE sfc_head IN %(data)s"
        self.cursor.execute(query, {'data': tuple(data)})
        results = self.cursor.fetchall()

        for row in results:
            print(row)

    def merge_duplicate(self):
        return 0


