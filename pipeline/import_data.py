import os
from pathlib import Path
import numpy as np
import pandas as pd
import laspy

from pcsfc.encoder import compute_split_length, las2csv, las2csv_full
from db import PgDatabase


class FileLoader:
    def __init__(self, path, ratio, name, srid):
        self.path = path
        self.ratio = ratio
        self.name = name
        self.srid = srid

        self.head_len = None
        self.tail_len = None

        self.meta = self.get_metadata()
        print(self.meta)

    def get_metadata(self):
        # name, srid, point_count, head_len, tail_len, scale, offset, bbox
        scale, offset = [1, 1, 1], [0, 0, 0]
        with laspy.open(self.path) as f:
            point_count = f.header.point_count
            bbox = [f.header.x_min, f.header.x_max, f.header.y_min, f.header.y_max, f.header.z_min, f.header.z_max]
            self.head_len, self.tail_len = compute_split_length(int(f.header.x_max), int(f.header.y_max), self.ratio)

        meta = [self.name, self.srid, point_count, self.ratio, scale, offset, bbox]
        return meta

    def preparation(self):
        las2csv(self.path, self.tail_len)

    def loading(self, db_conf):
        db = PgDatabase(db_conf["dbname"], db_conf["user"], db_conf["password"], db_conf["host"], db_conf["port"])
        db.connect()

        db.create_table(self.name)
        db.execute_sql(f"INSERT INTO pc_metadata_{self.name} VALUES (%s, %s, %s, %s, %s, %s, %s);", self.meta)
        db.execute_copy("pc_record.csv", self.name)
        db.create_btree_index(self.name)

        db.disconnect()


class FullResoLoader:
    def __init__(self, path, ratio, name, srid):
        self.path = path
        self.ratio = ratio
        self.name = name
        self.srid = srid

        self.head_len = None
        self.tail_len = None

        self.meta = self.get_metadata()
        print(self.meta)

    def get_metadata(self):
        # name, srid, point_count, head_len, tail_len, scale, offset, bbox
        scale, offset = [0.01, 0.01, 0.01], [0, 0, 0]
        with laspy.open(self.path) as f:
            point_count = f.header.point_count
            bbox = [f.header.x_min, f.header.x_max, f.header.y_min, f.header.y_max, f.header.z_min, f.header.z_max]
            self.head_len, self.tail_len = compute_split_length(int(f.header.x_max*100), int(f.header.y_max*100), self.ratio)

        meta = [self.name, self.srid, point_count, self.ratio, scale, offset, bbox]
        return meta

    def preparation(self):
        las2csv_full(self.path, self.tail_len)

    def loading(self, db_conf):
        db = PgDatabase(db_conf["dbname"], db_conf["user"], db_conf["password"], db_conf["host"], db_conf["port"])
        db.connect()

        db.create_table(self.name)
        db.execute_sql(f"INSERT INTO pc_metadata_{self.name} VALUES (%s, %s, %s, %s, %s, %s, %s);", self.meta)
        db.execute_copy("pc_record.csv", self.name)
        db.create_btree_index(self.name)

        db.disconnect()

class DirLoader:
    def __init__(self, path, ratio, name, srid):
        self.path = path
        self.ratio = ratio
        self.name = name
        self.srid = srid

        self.path_list = None
        self.head_len = None
        self.tail_len = None

        self.meta = self.get_metadata()
        print(self.meta)

    def get_metadata(self):
        # 1. Get a list of files
        files = self.get_file_names()
        self.path_list = [self.path + "/" + file for file in files]

        scale, offset = [1, 1, 1], [0, 0, 0]

        # 2. Iterate each file, read the header and extract point cloud and bbox
        with laspy.open(self.path_list[0]) as f:
            point_count = f.header.point_count
            x_min, y_min, z_min = f.header.x_min, f.header.y_min, f.header.z_min
            x_max, y_max, z_max = f.header.x_max, f.header.y_max, f.header.z_max

        for i in range(1, len(self.path_list)):
            with laspy.open(self.path_list[i]) as f:
                point_count += f.header.point_count
                x_min = min(x_min, f.header.x_min)
                x_max = max(x_max, f.header.x_max)
                y_min = min(y_min, f.header.y_min)
                y_max = max(y_max, f.header.y_max)
                z_min = min(z_min, f.header.z_min)
                z_max = max(z_max, f.header.z_max)
        bbox = [x_min, x_max, y_min, y_max, z_min, z_max]

        # 3. Based on the bbox of the whole point cloud, determine head_length and tail_length
        self.head_len, self.tail_len = compute_split_length(int(x_min), int(y_max), self.ratio)
        meta = [self.name, self.srid, point_count, self.ratio, scale, offset, bbox]
        return meta

    def preparation(self):
        for i in range(len(self.path_list)):
            filename = f"pc_record_{i}.csv"
            las2csv(self.path_list[i], self.tail_len, filename)

    def loading(self, db_conf):
        db = PgDatabase(db_conf["dbname"], db_conf["user"], db_conf["password"], db_conf["host"], db_conf["port"])
        db.connect()

        db.create_table(self.name)
        db.execute_sql(f"INSERT INTO pc_metadata_{self.name} VALUES (%s, %s, %s, %s, %s, %s, %s);", self.meta)

        for i in range(len(self.path_list)):
            filename = f"pc_record_{i}.csv"
            db.execute_copy(filename, self.name)

        db.create_btree_index(self.name)
        db.disconnect()

    def get_file_names(self):
        directory_path = Path(self.path)
        if not directory_path.is_dir():
            print(f"Error: {directory_path} is not a valid directory.")
            return []

        file_names = [file_path.name for file_path in directory_path.glob('*') if file_path.is_file()]
        return file_names
