import argparse
import time
from pipeline.import_data import file_importer, dir_importer


def main():
    parser = argparse.ArgumentParser(description="Your script description")
    subparsers = parser.add_subparsers(title="Available modes", dest="mode")

    # Mode 1: a file
    parser_file = subparsers.add_parser("file", help="single-file mode help")
    parser_file.add_argument("-p", "--path", type=str, default="/work/tmp/cynthia/bench_000210m/ahn_bench00210.las", help="the path to the input file")
    parser_file.add_argument("-r", "--ratio", type=float, default=0.7, help="the ratio to split the sfc key")
    parser_file.add_argument("-n", "--name", type=str, default="delft", help="the name of the point cloud")
    parser_file.add_argument("-c", "--crs", type=str, default="EPSG:28992", help="the Spatial Reference System of the point cloud")
    parser_file.add_argument("-U", "--user", type=str, default="cynthia", help='database username')
    parser_file.add_argument("-k", "--key", type=str, default="123456", help='database password')
    parser_file.add_argument("-h", "--host", type=str, default="localhost", help='database host')
    parser_file.add_argument("-d", "--db", type=str, default="cynthia", help='database name')
    parser_file.set_defaults(func=file_importer)

    # Mode 2:a directory
    parser_dir = subparsers.add_parser("dir", help="directory mode help")
    parser_dir.add_argument("-p", "--path", type=str, default="/work/tmp/cynthia/bench_000210m/ahn_bench00210.las", help="the path to the input file")
    parser_dir.add_argument("-r", "--ratio", type=float, default=0.7, help="the ratio to split the sfc key")
    parser_dir.add_argument("-n", "--name", type=str, default="delft", help="the name of the point cloud")
    parser_dir.add_argument("-c", "--crs", type=str, default="EPSG:28992", help="the Spatial Reference System of the point cloud")
    parser_dir.add_argument("-U", "--user", type=str, default="cynthia", help='database username')
    parser_dir.add_argument("-k", "--key", type=str, default="123456", help='database password')
    parser_dir.add_argument("-h", "--host", type=str, default="localhost", help='database host')
    parser_dir.add_argument("-d", "--db", type=str, default="cynthia", help='database name')
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
    main()
    run_time = time.time() - start_time
    print("The total running time:", run_time)