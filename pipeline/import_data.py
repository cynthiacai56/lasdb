import os
from pathlib import Path
import numpy as np
import pandas as pd
import laspy

from psycopg2 import connect, Error

from pcsfc.encoder import compute_split_length, process_point, make_groups
from db import PgDatabase


def dir_importer(args):
    path, ratio = args.path, args.ratio
    name, srid = args.name, args.srid
    db_conf = {
        "dbname": args.db,
        "user": args.user,
        "password": args.key,
        "host": "localhost",
        "port": 5432
    }

    # Process and load the metadata
    meta = MetaProcessor(path, ratio, name, srid)
    meta.get_meta_dir()
    meta.store_in_db(db_conf)
    new_path = meta.new_path
    tail_len = meta.meta[4]
    print(meta.meta)

    # Process and load the points
    for input_path in new_path:
        importer = PointGroupProcessor(input_path, tail_len)
        importer.import_db(db_conf)

    # TODO: Merge duplicate sfc_head


def file_importer(args):
    # Load parameters
    path, ratio = args.path, args.ratio
    name, srid = args.name, args.srid
    db_conf = {
        "dbname": args.db,
        "user": args.user,
        "password": args.key,
        "host": "localhost",
        "port": 5432
    }

    # Load metadata
    meta = MetaProcessor(path, ratio, name, srid)
    meta.get_meta_file()
    meta.store_in_db(db_conf)
    tail_len = meta.meta[4]
    print(meta.meta)

    # Read, encode and group the points
    importer = PointGroupProcessor(path, tail_len)
    importer.import_db(db_conf)

class MetaProcessor:
    def __init__(self, path, ratio, name, srid):
        self.path = path
        self.ratio = ratio
        self.name = name
        self.srid = srid
        self.meta = None
        self.new_path = None

    def get_meta(self, mode):
        if mode == "file":
            self.meta = self.get_meta_file()
        elif mode == "dir":
            self.meta = self.get_meta_dir()

    def get_meta_file(self):
        with laspy.open(self.path) as f:
            point_count = f.header.point_count
            head_len, tail_len = compute_split_length(int(f.header.x_max), int(f.header.y_max), self.ratio)
            bbox = [f.header.x_min, f.header.x_max, f.header.y_min, f.header.y_max, f.header.z_min, f.header.z_max]

        meta = [self.name, self.srid, point_count, head_len, tail_len, bbox]
        return meta

    def get_meta_dir(self):
        # 1. Get a list of files
        files = self.get_file_names()
        self.new_path = [self.path + "/" + file for file in files]

        # 2. Iterate each file, read the header and extract point cloud and bbox
        with laspy.open(self.new_path[0]) as f:
            point_count = f.header.point_count
            x_min, y_min, z_min = f.header.x_min, f.header.y_min, f.header.z_min
            x_max, y_max, z_max = f.header.x_max, f.header.y_max, f.header.z_max

        for i in range(1, len(self.new_path)):
            with laspy.open(self.new_path[i]) as f:
                point_count += f.header.point_count
                x_min = min(x_min, f.header.x_min)
                x_max = max(x_max, f.header.x_max)
                y_min = min(y_min, f.header.y_min)
                y_max = max(y_max, f.header.y_max)
                z_min = min(z_min, f.header.z_min)
                z_max = max(z_max, f.header.z_max)
        bbox = [x_min, x_max, y_min, y_max, z_min, z_max]

        # 3. Based on the bbox of the whole point cloud, determine head_length and tail_length
        head_len, tail_len = compute_split_length(int(x_min), int(y_max), self.ratio)
        meta = [self.name, self.srid, point_count, head_len, tail_len, bbox]
        return meta

    def store_in_db(self, db_conf):
        db = PgDatabase(db_conf["dbname"], db_conf["user"], db_conf["password"], db_conf["host"], db_conf["port"])
        db.connect()
        db.create_table(self.name)
        db.execute_sql(f"INSERT INTO pc_metadata_{self.name} VALUES (%s, %s, %s, %s, %s, %s);", self.meta)
        db.disconnect()

    def get_file_names(self):
        directory_path = Path(self.path)
        if not directory_path.is_dir():
            print(f"Error: {directory_path} is not a valid directory.")
            return []

        file_names = [file_path.name for file_path in directory_path.glob('*') if file_path.is_file()]
        return file_names


class PointGroupProcessor:
    def __init__(self, path, tail_len):
        with laspy.open(path) as f:
            point_count = f.header.point_count

        if point_count <= 50000000:
            self.process_points(path, tail_len)
        else:
            self.process_points_chunk(path, tail_len)

    def process_points(self, path, tail_len):
        las = laspy.read(path)
        points = np.vstack((las.x, las.y, las.z)).transpose()
        encoded_pts = [process_point(pt, tail_len) for pt in points]

        pc_groups = make_groups(encoded_pts)
        self.write_csv(pc_groups)

    def process_points_chunk(self, path, tail_len):
        encoded_pts = []
        with laspy.open(path) as f:
            for points in f.chunk_iterator(50000000):
                pts = np.vstack((points.x, points.y, points.z)).transpose()
                encoded_pts = encoded_pts + [process_point(pt, tail_len) for pt in pts]

        pc_groups = make_groups(encoded_pts)
        self.write_csv(pc_groups)

    def write_csv(self, pc_groups):
        df = pd.DataFrame(pc_groups, columns=['sfc_head','sfc_tail','z'])
        df['sfc_tail'] = df['sfc_tail'].apply(lambda x: str(x).replace('[', '{').replace(']', '}'))
        df['z'] = df['z'].apply(lambda x: str(x).replace('[', '{').replace(']', '}'))
        df.to_csv("pc_record.csv", index=False, mode='w')

    def import_db(self, db_conf, name):
        db = PgDatabase(db_conf["dbname"], db_conf["user"], db_conf["password"], db_conf["host"], db_conf["port"])
        db.connect()
        table_name = "pc_record_" + name
        db.execute_copy("pc_record.csv", name)
        db.disconnect()
