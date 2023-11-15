import numpy as np
import pandas as pd
import laspy
from itertools import groupby
from numba import jit, int32, int64


###############################################################################
######################      Morton conversion in 2D      ######################
###############################################################################

@jit(int64(int32))
def Expand2D(n):
    """
    Encodes the 64 bit morton code for a 31 bit number in the 2D space using
    a divide and conquer approach for separating the bits.
    1 bit is not used because the integers are not unsigned

    Args:
        n (int): a 2D dimension

    Returns:
        int: 64 bit morton code in 2D

    Raises:
        Exception: ERROR: Morton code is valid only for positive numbers
    """
    if n < 0:
        raise Exception("""ERROR: Morton code is valid only for positive numbers""")

    b = n & 0x7fffffff
    b = (b ^ (b << 16)) & 0x0000ffff0000ffff
    b = (b ^ (b << 8)) & 0x00ff00ff00ff00ff
    b = (b ^ (b << 4)) & 0x0f0f0f0f0f0f0f0f
    b = (b ^ (b << 2)) & 0x3333333333333333
    b = (b ^ (b << 1)) & 0x5555555555555555
    return b

@jit(int64(int32, int32))
def EncodeMorton2D(x, y):
    """
    Calculates the 2D morton code from the x, y dimensions

    Args:
        x (int): the x dimension
        y (int): the y dimension

    Returns:
        int: 64 bit morton code in 2D

    """
    return Expand2D(x) + (Expand2D(y) << 1)


def process_point(point, tail_len):
    mkey = EncodeMorton2D(int(point[0]), int(point[1]))
    head = mkey >> tail_len
    tail = mkey - (head << tail_len)
    encoded_point = [head, tail, float(point[2])]
    return encoded_point


def compute_split_length(x, y, ratio):
    mkey = EncodeMorton2D(x, y)
    length = len(bin(mkey)) - 2

    head_len = int(length * ratio)
    if head_len % 2 != 0:
        head_len = head_len - 1

    tail_len = length - head_len
    print(f"Key length | full: {length}, head: {head_len}, tail: {tail_len}")
    return head_len, tail_len


def make_groups(lst):
    # group the list by the first element of each sublist
    sorted_list = sorted(lst, key=lambda x: x[0])
    groups = groupby(sorted_list, lambda x: x[0])

    # print the groups
    zz = []
    for key, group in groups:
        group = list(group)
        yy = [key]
        for j in range(1, len(group[0])):
            xx = [group[i][j] for i in range(len(group))]
            yy.append(xx)
        zz.append(yy)

    return zz


def las2csv(path, tail_len, filename="pc_record.csv"):
    max_count = 500000000

    # Encode Morton key for all points
    with laspy.open(path) as f:
        point_count = f.header.point_count

    if point_count < max_count:
        las = laspy.read(path)
        points = np.vstack((las.x, las.y, las.z)).transpose()
        encoded_pts = [process_point(pt, tail_len) for pt in points]
    else:
        encoded_pts = []
        with laspy.open(path) as f:
            for points in f.chunk_iterator(max_count):
                pts = np.vstack((points.x, points.y, points.z)).transpose()
                encoded_pts = encoded_pts + [process_point(pt, tail_len) for pt in pts]

    # Sort and group the points
    pc_groups = make_groups(encoded_pts)

    # Write the point group in csv file
    df = pd.DataFrame(pc_groups, columns=['sfc_head','sfc_tail','z'])
    df['sfc_tail'] = df['sfc_tail'].apply(lambda x: str(x).replace('[', '{').replace(']', '}'))
    df['z'] = df['z'].apply(lambda x: str(x).replace('[', '{').replace(']', '}'))
    df.to_csv(filename, index=False, mode='w')


def las2csv_full(path, tail_len, filename="pc_record.csv"):
    max_count = 500000000

    # Encode Morton key for all points
    with laspy.open(path) as f:
        point_count = f.header.point_count

    if point_count < max_count:
        las = laspy.read(path)
        points = np.vstack((las.X, las.Y, las.Z)).transpose()
        encoded_pts = [process_point(pt, tail_len) for pt in points]
    else:
        encoded_pts = []
        with laspy.open(path) as f:
            for points in f.chunk_iterator(max_count):
                pts = np.vstack((points.X, points.Y, points.Z)).transpose()
                encoded_pts = encoded_pts + [process_point(pt, tail_len) for pt in pts]

    # Sort and group the points
    pc_groups = make_groups(encoded_pts)

    # Write the point group in csv file
    df = pd.DataFrame(pc_groups, columns=['sfc_head','sfc_tail','z'])
    df['sfc_tail'] = df['sfc_tail'].apply(lambda x: str(x).replace('[', '{').replace(']', '}'))
    df['z'] = df['z'].apply(lambda x: str(x).replace('[', '{').replace(']', '}'))
    df.to_csv(filename, index=False, mode='w')