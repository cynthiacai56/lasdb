import os
import time
import numpy as np
import pandas as pd
import laspy

from pcsfc.point_processor import compute_split_length, PointProcessor
from db import Postgres


class FileLoader:
    def __init__(self, name, dict):
        self.name = name
        self.path = dict["path"]
        self.srid = dict["srid"]
        self.ratio = dict["ratio"]

        self.scales = dict["scales"]
        self.offsets = dict["offsets"]

        self.head_len = None
        self.tail_len = None

        self.meta = self.get_metadata()
        print(self.meta)

    def get_metadata(self):
        # name, srid, point_count, head_len, tail_len, scale, offset, bbox
        with laspy.open(self.path) as f:
            point_count = f.header.point_count
            bbox = [f.header.x_min, f.header.x_max, f.header.y_min, f.header.y_max, f.header.z_min, f.header.z_max]

            X_max = round((f.header.x_max - self.offsets[0]) / self.scales[0])
            Y_max = round((f.header.y_max - self.offsets[1]) / self.scales[1])
            self.head_len, self.tail_len = compute_split_length(X_max, Y_max, self.ratio)

        meta = [self.name, self.srid, point_count, round(self.ratio, 2), self.scales, self.offsets, bbox]
        return meta

    def preparation(self):
        processor = PointProcessor(self.path, self.tail_len, self.scales, self.offsets)
        processor.execute()

    def loading(self, db_conf):
        db = Postgres(db_conf, self.name)
        db.load(self.meta, f"./cache/{self.name}.csv")


class DirLoader:
    def __init__(self, name, dict):
        self.name = name
        self.paths = self.get_file_paths(dict["path"])
        self.srid = dict["srid"]
        self.ratio = dict["ratio"]

        self.scales = dict["scales"]
        self.offsets = dict["offsets"]

        self.head_len = None
        self.tail_len = None
        self.csv_list = None

        self.meta = self.get_metadata()
        print(self.meta)

    def get_metadata(self):
        # 1. Iterate each file, read the header and extract point cloud and bbox
        with laspy.open(self.paths[0]) as f:
            point_count = f.header.point_count
            x_min, y_min, z_min = f.header.x_min, f.header.y_min, f.header.z_min
            x_max, y_max, z_max = f.header.x_max, f.header.y_max, f.header.z_max

        scales, offsets = [1, 1, 1], [0, 0, 0]

        for i in range(1, len(self.paths)):
            with laspy.open(self.paths[i]) as f:
                point_count += f.header.point_count
                x_min = min(x_min, f.header.x_min)
                x_max = max(x_max, f.header.x_max)
                y_min = min(y_min, f.header.y_min)
                y_max = max(y_max, f.header.y_max)
                z_min = min(z_min, f.header.z_min)
                z_max = max(z_max, f.header.z_max)
        bbox = [x_min, x_max, y_min, y_max, z_min, z_max]

        # 2. Based on the bbox of the whole point cloud, determine head_length and tail_length
        self.head_len, self.tail_len = compute_split_length(round(x_min), round(y_max), self.ratio)
        meta = [self.name, self.srid, point_count, self.ratio, self.scales, self.offsets, bbox]
        return meta

    def preparation(self):
        csv_list = []
        for i in range(len(self.paths)):
            filename = f"./cache/pc_record_{i}.csv"
            csv_list.append(filename)
            processor = PointProcessor(self.paths[i], self.tail_len, self.scales, self.offsets)
            processor.execute(filename)
            print(filename, 'saved.')
        self.csv_list = csv_list

    def loading(self, db_conf):
        db = Postgres(db_conf, self.name)
        db.load(self.meta, self.csv_list)

    def get_file_paths(self, dir_path):
        return [os.path.join(dir_path, file) for file in os.listdir(dir_path) if
                      os.path.isfile(os.path.join(dir_path, file))]