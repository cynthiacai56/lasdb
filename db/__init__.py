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

    def create_table(self):
        if not self.connection:
            print("Error: Database connection is not established.")
            return

        create_table_sql = """
            CREATE TABLE IF NOT EXISTS pc_metadata_2201m (
                name TEXT,
                crs TEXT,
                point_count INT,
                head_length INT,
                tail_length INT,
                bbox DOUBLE PRECISION[]
            );        
            CREATE TABLE IF NOT EXISTS pc_record_2201m (
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

    def check_exist(self, file):
        # TODO: it needs correction
        table_name = 'pc_metadata_2201m'
        query = f"SELECT 1 FROM {table_name} WHERE source_file = %s;"
        self.cursor.execute(query, (file,))
        result = self.cursor.fetchone()
        if result is not None:
            return 0
        else:
            return 1

    def execute_query(self, sql, data=None):
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

    def query_with_points(self, data):
        query = "SELECT column_name FROM your_table WHERE column_name IN %s"
        self.cursor.execute(query, (tuple(data),))

        rows = self.cursor.fetchall()
        results = [row for row in rows]
        return results

    def execute_copy(self, filename):
        if not self.connection:
            print("Error: Database connection is not established.")
            return

        # Use the COPY command to insert point records into the table
        with open(filename, 'r') as f:
            try:
                self.cursor.copy_expert(sql="COPY pc_record_2201m FROM stdin WITH CSV HEADER", file=f)
                self.connection.commit()
                print("Data copied successfully.")
            except Error as e:
                print("Error: Unable to copy the data.")
                print(e)
                self.connection.rollback()

    def merge_duplicate(self):
        sql = """
        WITH duplicates AS (
            SELECT sfc_head, array_agg(sfc_tail), array_agg(z) as ids
            FROM pc_record
            GROUP BY sfc_head
            HAVING COUNT(*) > 1
        )
        UPDATE pc_record t
        SET 
            sfc_tail = (
            SELECT ARRAY(
                SELECT DISTINCT unnest(array_agg(sfc_tail))
            )
            FROM pc_record
            WHERE sfc_head = t.sfc_head
            ),
            z = (
            SELECT ARRAY(
                SELECT DISTINCT unnest(array_agg(z))
            )
            FROM pc_record
            WHERE sfc_head = t.sfc_head
            )
        WHERE EXISTS (
            SELECT 1 FROM duplicates d WHERE d.sfc_head = t.sfc_head
        );
        """
        try:
            self.cursor.execute(sql)
            self.connection.commit()
        except Error as e:
            print(f"Error: Unable to execute query: {sql}")
            print(e)
            self.connection.rollback()


