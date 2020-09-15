#!/software/mobi/crispr-manager/2.0.0/bin/python

import argparse, json, time, logging
import pycouch.wrapper as couch_wrapper

logging.basicConfig(level = logging.INFO)


def args_gestion():
    parser = argparse.ArgumentParser(description = "To use backup motifs database or use current version. Will modify the mapping rules")

    subparsers = parser.add_subparsers(dest='subparser_name')

    parser_backup = subparsers.add_parser('use_backup', help='Use backup databases')
    parser_backup.add_argument("-m", "--mapping", help = "current mapping rules", required = True, type=str)
    parser_backup.add_argument("-v", "--version", help = "backup version (1 is latest, 2 is second latest...) (default : 1)", default = 1, type = int)
    parser_backup.add_argument("--url", help = "couchDB endpoint", required = True, type=str)

    parser_current = subparsers.add_parser("use_current", help="Use current databases")
    parser_current.add_argument("-m", "--mapping", help = "current mapping rules", required = True, type=str)
    parser_current.add_argument("--url", help = "couchDB endpoint", required = True, type=str)

    args = parser.parse_args()
    if not args.subparser_name:
        parser.print_help()
        exit()
    return parser.parse_args()

def backup_to_current(mapping):
    new_mapping = {}
    for motif,db in mapping.items():
        new_mapping[motif] = db.split("-")[0]

    #Check if exists in db I guess
    return new_mapping

def get_version(backups, version:int):
    #This function is kind of redondant with one in replicate_database.py. Create a module one day.
    single_backup = {}
    for (db, backup) in backups.items(): 
        timesteps = [bk.split("bak")[1] for bk in backup]
        sorted_timesteps = sorted(timesteps, reverse = True)
        
        if version > len(sorted_timesteps):
            print(f"WARN : impossible to restore {db} from version {version}. There's not enough version")

        else:
            backup_idx = timesteps.index(sorted_timesteps[version - 1])
            single_backup[db] = backup[backup_idx]

    return single_backup

def current_to_backup(mapping, version):
    new_mapping = {}
    all_dbs = COUCHDB.couchDbList()
    backups = {db : [all_db for all_db in all_dbs if all_db.startswith(db + "-")] for db in mapping.values()}
    single_backup = get_version(backups, ARGS.version)
    for motif,db in mapping.items():
        new_mapping[motif] = single_backup[db]
    return new_mapping

if __name__ == "__main__":
    ARGS = args_gestion()
    TIMESTAMP = time.strftime("%Y%m%d-%H%M%S")
    
    logging.info("Read mapping...")
    mapping = json.load(open(ARGS.mapping))
    logging.info("Keep backup of mapping rules...")
    if "/" in ARGS.mapping:
        mapping_dir = "/".join(ARGS.mapping.split("/"))
        mapping_file = ARGS.mapping.split("/")[-1].split(".")[0]
    else:
        mapping_dir = "."
        mapping_file = ARGS.mapping.split(".")[0]
    
    save = f"{mapping_dir}/{mapping_file}-{TIMESTAMP}.json"
    json.dump(mapping, open(save, "w"))
    logging.info(f"Current mapping save in {save}")

    logging.info("Access couchDB database...")
    COUCHDB = couch_wrapper.Wrapper(ARGS.url)
    if not COUCHDB.couchPing():
        raise Exception("Can't ping database")


    logging.info("Modify mapping rules...")
    if ARGS.subparser_name == "use_current":
        new_mapping = backup_to_current(mapping)
    
    elif ARGS.subparser_name == "use_backup":
        new_mapping = current_to_backup(mapping, ARGS.version)

    logging.info("Write new mapping rules...")
    json.dump(new_mapping, open(ARGS.mapping,"w"))

    logging.info(f"New mapping rules in {ARGS.mapping}")
    logging.info("END - Relaunch motif-broker with this new mapping rules")

    




