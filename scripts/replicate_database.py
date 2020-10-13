#!/software/mobi/crispr-manager/2.0.0/bin/python

import pycouch.wrapper as couch_wrapper
import argparse
import re
import sys
import time
import copy
import sys 
import os
import requests
import json
import CSTB_database_manager.engine.watch_replication as watch

SESSION = requests.session()
SESSION.trust_env = False
FAILED = []
TIMESTAMP = time.strftime("%Y%m%d-%H%M%S")

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

def args_gestion():
    parser = argparse.ArgumentParser(description = "Replicate couchDB database")
    
    subparsers = parser.add_subparsers(dest='subparser_name')
    parser_backup = subparsers.add_parser('backup', help='Create backup for databases')
    parser_backup.add_argument("--db", metavar="<str>", help = "Replicate database(s) corresponding to this regular expression")
    parser_backup.add_argument("--all", help = "Replicate all databases in couchDB", action="store_true")
    parser_backup.add_argument("--url", metavar="<str>", help = "couchDB endpoint", required = True)
    parser_backup.add_argument("--bulk", metavar="<int>", help = "Number of replication to launch simultanously (default: 2)", default=2)

    parser_restore = subparsers.add_parser('restore', help = "Restore database from a backup version")
    parser_restore.add_argument("--version", type = int, help = "Which backup version do you want to restore ? 1 for the latest, 2 for the second latest... (default : 1)", default = 1)
    parser_restore.add_argument("--db", metavar="<str>", help = "Replicate database(s) corresponding to this regular expression")
    parser_restore.add_argument("--all", help = "Replicate all databases in couchDB", action="store_true")
    parser_restore.add_argument("--url", metavar="<str>", help = "couchDB endpoint", required = True)
    parser_restore.add_argument("--bulk", metavar="<int>", help = "Number of replication to launch simultanously (default: 2)", default=2)

    args = parser.parse_args()

    if not args.subparser_name:
        parser.print_help()
        exit()

    if args.db and args.all:
        print("You have to choose between --db or --all")
        exit()
    if not args.db and not args.all:
        print("You have to give --all or --db argument")
        exit()
    args.url = args.url.rstrip("/")
    args.bulk = int(args.bulk)
    return args    

def get_database_names():
    db_names = [db_name for db_name in couchDB.couchGetRequest("_all_dbs") if not db_name.startswith("_")]
    return db_names

def get_replicate_doc(db_names, url):
    docs = {}
    for name in db_names:
        if ARGS.subparser_name == "backup" : 
            target = name + "-bak" + TIMESTAMP
        elif ARGS.subparser_name == "restore":
            target = name.split("-")[0]
        
        rep_id = "rep_" + target
        docs[rep_id] = {"source": url + "/" + name, "target" : url + "/" + target, "create_target": True, "continuous": False}
    return docs

def get_regexp(input_str):
    regexp = "^" + input_str.replace("*", ".*") + "$"
    return re.compile(regexp)

def monitor_replication(insert_ids, sleep_time = 5):
    completed_ids = set()
    running_ids = copy.deepcopy(insert_ids)
    while set(insert_ids) != completed_ids:
        print("Check status...")
        get_results = couchDB.bulkRequestByKey(running_ids, "_replicator")["results"]
        for rows in get_results:
            doc = rows["docs"][0]["ok"]
            if doc.get("_replication_state") == "completed":
                completed_ids.add(doc["_id"])
                running_ids.remove(doc["_id"])
                print(doc["_id"], "replication job is complete.")
        time.sleep(2)    

def create_bulks(db_names, bulk_size):
    bulks = [db_names[x:x+bulk_size] for x in range(0, len(db_names), bulk_size)]
    return bulks

def delete_replication_doc(*repIDs):
    global ARGS
    for rep in repIDs:
        doc_url = ARGS.url + "/_replicator/" + rep
        rev_id = json.loads(SESSION.get(doc_url).text)["_rev"]
        print("Delete " + rep + " replication document...")
        res = SESSION.delete(doc_url + "?rev=" + rev_id)
        print(res.text)

def delete_databases(*repIDs):
    for rep in repIDs:
        target = rep.lstrip("rep_")
        res_json = json.loads(SESSION.get(ARGS.url + "/" + target).text)
        if "error" in res_json and res_json["error"] == "not_found":
            continue
        print("Delete " + target + "...")
        res = SESSION.delete(ARGS.url + "/" + target)
        print(res.text)

def replication(databases):
    global FAILED
    print("I replicate", databases)   
    to_insert = get_replicate_doc(databases, ARGS.url)
    couchDB.bulkDocAdd(to_insert, target = "_replicator")

    repIDs = [rep_name for rep_name in to_insert]

    try:
        watch_failed = watch.launch(*repIDs)
    except KeyboardInterrupt:
        print("KeyboardInterrupt")
        delete_replication_doc(*repIDs)
        FAILED += repIDs
        return
        
    FAILED = watch_failed

    for rep in repIDs:
        if rep in FAILED:
            print("I fail replicate", rep)
            
        else:
            print("I finish replicate", rep)

def get_version(backups, version:int):
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


if __name__ == '__main__':
    ARGS = args_gestion()

    couchDB = couch_wrapper.Wrapper(ARGS.url)
    if not couchDB.couchPing():
        raise Exception("Can't ping database")

    all_dbs = couchDB.couchDbList()

    if ARGS.db:
        regExp = get_regexp(ARGS.db)
        db_names = [db_name for db_name in all_dbs if regExp.match(db_name)]

    if not db_names:
        print(f"No database to {ARGS.subparser_name}")
        exit()

    if ARGS.subparser_name == "restore":
        backups = {db : [all_db for all_db in all_dbs if all_db.startswith(db + "-")] for db in db_names}
        single_backup = get_version(backups, ARGS.version)
        db_names = list(single_backup.values())

    print(f"== Databases to {ARGS.subparser_name}:")
    for db_name in db_names:
        print(db_name)

    confirm = input(f"Do you want to {ARGS.subparser_name} this databases ? (y/n) ")
    while (confirm != "y" and confirm != "n"):
        confirm = input("I need y or n answer : ") 

    if confirm == "n":
        exit()

    if ARGS.subparser_name == "restore":
        print("Delete database to rewrite it")
        for db in single_backup:
            res = json.loads(requests.delete(ARGS.url + "/" + db).text)
            if not "ok" in res:
                raise Exception(f"Error while delete {db} : {res}")


    bulks = create_bulks(db_names, ARGS.bulk)
    watch.setServerURL(ARGS.url)
    
    nb_bulk = 0
    for bulk in bulks:
        nb_bulk += 1
        watch.setLogStatus(TIMESTAMP + "_replicate_bulk" + str(nb_bulk)+ "_status.log")
        watch.setLogRunning(TIMESTAMP + "_replicate_bulk" + str(nb_bulk)+ "_running.log")
        replication(bulk)
        print()

    if FAILED:
        print("== Failed", FAILED)
        print("Don't forget to delete newly created failed databases")
    else:
        print("== All databases successfully replicated")
    
    final_log = TIMESTAMP + "_end.log"
    with open(final_log, "w") as o:
        if not FAILED:
            o.write("All databases successfully replicated")
        else:
            o.write("Failed databases : \n")
            for f in FAILED:
                o.write(f + "\n")
        
    #print("== Monitor replication")
    #repIDs = [rep_name for rep_name in to_insert]
    #watch.setServerURL(ARGS.url)
    #if (ARGS.db):
    #    watch.setLogStatus(ARGS.db + "_status.log")
    #    watch.setLogRunning(ARGS.db + "_running.log")
    #watch.launch(*repIDs)

    
        
        

