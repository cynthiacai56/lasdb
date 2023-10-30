import argparse
import time
from pipeline.import_data import file_importer, dir_importer


def main():
    parser = argparse.ArgumentParser(description="Your script description")
    subparsers = parser.add_subparsers(title="Available modes", dest="mode")

    # Mode 1: a file
    parser_file = subparsers.add_parser("file", help="single-file mode help")
    parser_file.add_argument("-path", default="/work/tmp/cynthia/bench_000210m/ahn_bench00210.las", help="the path to the input file")
    parser_file.add_argument("-ratio", default=0.7, help="the ratio to split the sfc key")
    parser_file.add_argument("-name", default="delft", help="the name of the point cloud")
    parser_file.add_argument("-crs", default="EPSG:28992", help="the Spatial Coordinate System of the point cloud")
    parser_file.add_argument("-user", default='cynthia', help='database username')
    parser_file.add_argument("-key", default='123456', help='database password')
    parser_file.add_argument("-host", default='localhost', help='database host')
    parser_file.add_argument('-db', default='cynthia', help='database name')
    parser_file.set_defaults(func=file_importer)

    # Mode 2:a directory
    parser_dir = subparsers.add_parser("dir", help="directory mode help")
    parser_dir.add_argument("-path", default="/work/tmp/cynthia/bench_002201m", help="input path")
    parser_dir.add_argument("-ratio", default=0.7, help="the ratio to split the sfc key")
    parser_file.add_argument("-name", default="south_holland", help="the name of the point cloud")
    parser_file.add_argument("-crs", default="EPSG:28992", help="the Spatial Coordinate System of the point cloud")
    parser_dir.add_argument("-user", default='cynthia', help='database username')
    parser_dir.add_argument("-key", default='123456', help='database password')
    parser_dir.add_argument("-host", default='localhost', help='database host')
    parser_dir.add_argument('-db', default='cynthia', help='database name')
    #parser_dir.add_argument("-n", default=20, help="the number of imported files")
    parser_dir.set_defaults(func=dir_importer)

    args = parser.parse_args()
    if hasattr(args, "func"):
        start_time = time.time()
        args.func(args)
        run_time = time.time() - start_time
        print("The total running time:", run_time)
    else:
        parser.print_help()


if __name__ == "__main__":
    #main()
    start_time = time.time()
    test_dir()
    run_time = time.time() - start_time
    print("The total running time:", run_time)