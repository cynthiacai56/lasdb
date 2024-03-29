import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import laspy
from itertools import groupby
from collections import Counter

from pcsfc.encoder import EncodeMorton2D


def compute_split_length(x, y, ratio):
    mkey = EncodeMorton2D(x, y)
    length = len(bin(mkey)) - 2

    head_len = int(length * ratio)
    if head_len % 2 != 0:
        head_len = head_len - 1

    tail_len = length - head_len
    #print(f"Key length | full: {length}, head: {head_len}, tail: {tail_len}")
    return head_len, tail_len


class PointProcessor:
    def __init__(self, path, tail_len, scales=None, offsets=None):
        self.path = path
        self.tail_len = tail_len
        self.scales = scales
        self.offsets = offsets

    def execute(self, filename="pc_record.csv"):
        las = laspy.read(self.path)
        points = np.vstack((las.x, las.y, las.z)).transpose()
        encoded_pts = self.encode_split_points(points)

        # Sort and group the points
        pt_blocks = self.make_groups(encoded_pts)
        self.write_csv(pt_blocks, filename)


    def encode_split_points(self, points):
        encoded_points = []
        for point in points:
            # Scale and shift the XY coordinates
            x = round((point[0] - self.offsets[0]) / self.scales[0])# scales should not be 0
            y = round((point[1] - self.offsets[1]) / self.scales[1])
            z = round(point[2], 2)

            # Encode XY coordinates with Morton Curve
            mkey = EncodeMorton2D(x, y)

            # Split the Morton key into head and tail
            head = mkey >> self.tail_len
            tail = mkey - (head << self.tail_len)

            # Save the point
            encoded_points.append((head, tail, z))

        return encoded_points

    def make_groups(self, my_data):
        # Group the list by the first element of each sublist
        sorted_list = sorted(my_data, key=lambda x: x[0])  # Sort by SFC head
        groups = groupby(sorted_list, lambda x: x[0])

        # Pack the groups
        pt_blocks = []
        histogram = []
        for key, group in groups:
            sorted_group = sorted(list(group), key=lambda x: x[1])  # Sort by SFC tail
            n = len(sorted_group)
            sfc_tail = [sorted_group[i][1] for i in range(n)]
            z = [sorted_group[i][2] for i in range(n)]
            histogram.append((key, n))
            pt_blocks.append((key, sfc_tail, z))

        num_block = len(pt_blocks)
        df_hist = pd.DataFrame(histogram, columns=['head', 'num_tail'])
        df_hist.to_csv(f"histogram_{num_block}.csv", index=False)

        return pt_blocks

    def write_csv(self, pt_blocks, filename):
        df = pd.DataFrame(pt_blocks, columns=['sfc_head', 'sfc_tail', 'z'])
        df['sfc_tail'] = df['sfc_tail'].apply(lambda x: str(x).replace('[', '{').replace(']', '}'))
        df['z'] = df['z'].apply(lambda x: str(x).replace('[', '{').replace(']', '}'))
        df.to_csv(filename, index=False, mode='w')

