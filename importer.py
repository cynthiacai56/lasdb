import sys
import json
import time
import argparse
from pipeline.import_data import MetaProcessor, PointGroupProcessor


def main():
    parser = argparse.ArgumentParser(description='Example of argparse usage.')
    parser.add_argument('--input', type=str, default="./scripts/import.json", help='Input parameter json file path.')
    parser.add_argument('--password', type=str, default="123456", help='Input parameter json file path.')
    args = parser.parse_args()
    #jparams_path = "./scripts/import_local.json"
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
        print(f"=== Import {key} ===")
        start_time = time.time()
        # Load parameters
        mode = value["mode"]
        name, srid = key, value["srid"]
        path, ratio = value["path"], value["ratio"]

        # Read and import metadata
        # table name: "pc_metadata_" + name
        meta = MetaProcessor(path, ratio, name, srid)
        meta.get_meta(mode)
        meta.store_in_db(db_conf)
        tail_len = meta.meta[4]
        print(meta.meta)

        # Read, process and import point records
        # table name: "pc_record_" + name
        if mode == "file":
            importer = PointGroupProcessor(path, tail_len)
            importer.import_db(db_conf, name)
        elif mode == "dir":
            new_path = meta.new_path
            for input_path in new_path:
                importer = PointGroupProcessor(input_path, tail_len)
                importer.import_db(db_conf, name)
            # TODO: Merge duplicate sfc_head

        print("-->%ss" % round(time.time() - start_time, 2))


if __name__ == '__main__':
    main()