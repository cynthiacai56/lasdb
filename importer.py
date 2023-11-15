import sys
import json
import time
import argparse
from pipeline.import_data import FileLoader, DirLoader, FullResoLoader


def main():
    parser = argparse.ArgumentParser(description='Example of argparse usage.')
    parser.add_argument('--input', type=str, default="./scripts/import.json", help='Input parameter json file path.')
    parser.add_argument('--password', type=str, default="123456", help='Input parameter json file path.')
    args = parser.parse_args()
    #jparams_path = "./scripts/import_20m_local.json"
    jparams_path = args.input

    try:
        with open(jparams_path, 'r') as f:
            jparams = json.load(f)
    except FileNotFoundError:
        print("ERROR: File not found.")
    except json.JSONDecodeError as e:
        print(f"ERROR: JSON decoding error: {e}")
        sys.exit()

    db_conf = jparams["config"]
    db_conf["password"] = args.password

    for key, value in jparams["imports"].items():
        print(f"=== Import {key} into PostgreSQL===") # key is name
        start_time = time.time()

        try:
            if value["mode"] == "file":
                if value["resolution"] == "full":
                    pipeline = FullResoLoader(value["path"], value["ratio"], key, value["srid"])
                elif value["resolution"] == "compression":
                    pipeline = FileLoader(value["path"], value["ratio"], key, value["srid"])
                pipeline.preparation()
                initial_time = time.time()
                print("Initial time:", initial_time - start_time)
                pipeline.loading(db_conf)
                load_time = time.time()
                print("Load time:", load_time - initial_time)

            elif value["mode"] == "dir":
                pipeline = DirLoader(value["path"], value["ratio"], key, value["srid"])
                pipeline.preparation()
                initial_time = time.time()
                print("Initial time:", initial_time - start_time)
                pipeline.loading(db_conf)
                load_time = time.time()
                print("Load time:", load_time - initial_time)

        except Exception as e:
            print(f"An error occurred: {e}")

        print("-->%ss" % round(time.time() - start_time, 2))


if __name__ == '__main__':
    main()