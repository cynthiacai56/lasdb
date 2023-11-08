import numpy as np
import pandas as pd
import laspy

from psycopg2 import connect, Error, extras

from pcsfc.decoder import DecodeMorton2D
from pcsfc.range_search import morton_range


class Querier:
    def __init__(self, head_len, tail_len, db_conf, source_table, name):
        self.head_len = head_len
        self.tail_len = tail_len
        self.source_table = source_table
        self.name = name

        try:
            self.connection = connect(
                dbname=db_conf['dbname'],
                user=db_conf['user'],
                password=db_conf['password'],
                host=db_conf['host'],
                port=db_conf['port']
            )
            self.cursor = self.connection.cursor()
        except Error as e:
            print("Error: Unable to connect to the database.")
            print(e)


    def geometry_query(self, mode, geometry):
        if mode == "bbox":
            self.bbox_query(geometry)
        elif mode == "circle":
            self.circle_query(geometry)
        elif mode == "polygon":
            self.polygon_query(geometry)
        elif mode == "nn":
            print("nn search is not developed yet.")

    def bbox_query(self, bbox):
        self.range_search(bbox)

    def circle_query(self, geometry):
        # 1. Compute bounding box
        center_x, center_y, radius = geometry[0][0], geometry[0][1], geometry[1]
        x_min, x_max = center_x - radius, center_x + radius
        y_min, y_max = center_y - radius, center_y + radius
        bbox = [x_min, x_max, y_min, y_max]

        # 2. Range search based on bounding box and create table as intermediate result
        self.range_search(bbox)

        # 3. Use PostGIS function to query the points inside the circle, create table as result
        circle_query = f"""
            DELETE FROM {self.name}
            WHERE NOT ST_DWithin(point, ST_MakePoint({center_x}, {center_y}), {radius});
        """
        self.cursor.execute(circle_query)
        self.connection.commit()
        print(f"Circle search is updated in {self.name}.")

    def polygon_query(self, polygon_points):
        # 1. Compute bounding box
        x = [pt[0] for pt in polygon_points]
        y = [pt[1] for pt in polygon_points]
        x_min, x_max = min(x), max(x)
        y_min, y_max = min(y), max(y)
        bbox = [x_min, x_max, y_min, y_max]

        # 2. Range search based on bounding box and create table as intermediate result
        self.range_search(bbox)

        # 3. Use PostGIS function to query the points inside the circle, create table as result
        points_string = ','.join([f"{x} {y}" for x, y in polygon_points])
        polygon_query = f"""
            DELETE FROM {self.name}
            WHERE NOT ST_Within(point, ST_GeomFromText('POLYGON(({points_string}))'))
        """
        self.cursor.execute(polygon_query)
        self.connection.commit()
        print(f"Polygon search is updated in {self.name}.")

    def maxz_query(self, maxz):
        z_query = f"""
            DELETE FROM {self.name}
            WHERE ST_Z(point) > {maxz}
        """
        self.cursor.execute(z_query)
        self.connection.commit()
        print(f"Max height search is updated in {self.name}.")

    def minz_query(self, minz):
        z_query = f"""
            DELETE FROM {self.name}
            WHERE ST_Z(point) < {minz}
        """
        self.cursor.execute(z_query)
        self.connection.commit()
        print(f"Min height search is updated in {self.name} successfully.")

    def range_search(self, bbox):
        # 1. Find the fully containing and overlapping heads
        head_ranges, head_overlaps = morton_range(bbox, 0, self.head_len, self.tail_len)

        # 2. Take these heads out of the database
        ## 2.1 Range query
        # Create a range table and insert data
        self.cursor.execute('DROP TABLE IF EXISTS RangeTable')
        self.cursor.execute('''CREATE TEMP TABLE RangeTable (range_start INT, range_end INT)''')
        self.cursor.executemany('INSERT INTO RangeTable (range_start, range_end) VALUES (%s, %s)', head_ranges)

        self.cursor.execute(f'''
            SELECT * FROM {self.source_table} 
            WHERE EXISTS (
                SELECT 1 FROM RangeTable 
                WHERE {self.source_table}.sfc_head BETWEEN RangeTable.range_start AND RangeTable.range_end
            )
        ''')
        res1 = self.cursor.fetchall() # data type: a list of tuple ?

        ## 2.2 Overlaps Query
        self.cursor.execute(f'''SELECT * FROM {self.source_table} WHERE sfc_head = ANY(%s)''', (head_overlaps,))
        res2 = self.cursor.fetchall()

        # 3. Unpack the point block and decode
        points_within_bbox = []
        for (sfc_head, sfc_tail, z) in res1:
            for i in range(len(sfc_tail)):
                sfc_key = sfc_head << self.tail_len | sfc_tail[i]
                x, y = DecodeMorton2D(sfc_key)
                points_within_bbox.append([x, y, z[i]])

        for (sfc_head, sfc_tail, z) in res2:  # Each group
            # Check which tails of this head in within the ranges
            tail_rgs, tail_ols = morton_range(bbox, sfc_head, self.tail_len, 0)
            # Unpack the tails
            for i in range(len(sfc_tail)):  # Each point
                # Check if the tail in within the ranges
                check_in_range = any(start <= sfc_tail[i] <= end for start, end in tail_rgs)
                if check_in_range == 1:
                    sfc_key = sfc_head << self.tail_len | sfc_tail[i]
                    x, y = DecodeMorton2D(sfc_key)
                    points_within_bbox.append([x, y, z[i]])

        # 4. Create results as a table
        self.cursor.execute(f"CREATE TABLE {self.name} (point geometry(PointZ));")
        insert_sql = f"INSERT INTO {self.name} VALUES (ST_MakePoint(%s, %s, %s));"
        for point in points_within_bbox:
            self.cursor.execute(insert_sql, point)
        self.connection.commit()
        print(f"Points within the bounding box are inserted into the table '{self.name}'.")


    def disconnect(self):
        if self.connection:
            self.cursor.close()
            self.connection.close()
            self.connection = None
            self.cursor = None