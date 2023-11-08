import sys
import json
import time
import argparse

from pipeline.retrieve_data import Querier

def main():
    parser = argparse.ArgumentParser(description='Example of argparse usage.')
    parser.add_argument('--input', type=str, default="./scripts/query_20m.json", help='Input parameter json file path.')
    parser.add_argument('--password', type=str, default="123456", help='Input parameter json file path.')
    args = parser.parse_args()
    # jparams_path = "./scripts/query_20m_local.json"
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
    head_len, tail_len = 26, 12

    for key, value in jparams["queries"].items():
        start_time = time.time()
        source_table = "pc_record_" + value["source_dataset"]
        query_name, mode, geometry = key, value["mode"], value["geometry"]
        print(f"=== Query {key} from {source_table} ===")
        print(f"Name: {key}, Mode: {mode}, Geometry: {geometry}")

        # perform query
        query = Querier(head_len, tail_len, db_conf, source_table, query_name)
        query.geometry_query(mode, geometry)

        if "maxz" in value:
            query.maxz_query(value["maxz"])
        if "minz" in value:
            query.minz_query(value["minz"])

        query.disconnect()

        print("-->%ss" % round(time.time() - start_time, 2))


if __name__ == '__main__':
    main()