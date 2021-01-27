#!/software/mobi/crispr-manager/2.0.0/bin/python

import argparse
import os
import random
import CSTB_database_manager.databaseManager as dbManager
from datetime import date
import json
import subprocess
import logging
from check_consistency import process_sequences
logging.basicConfig(level = logging.INFO, format='%(levelname)s\t%(filename)s:%(lineno)s\t%(message)s')

DB_MANAGER_PATH = "/software/mobi/ms-db-manager/1.0.0"
DB_REGEX = "crispr_rc03_v[0-255]"

def args_gestion(): 
    parser = argparse.ArgumentParser(description="Randomly select genomes uuid that are not already in log")

    parser.add_argument("--config", help = "json config file (see config.json for format)", required = True,  type=str)
    parser.add_argument("--log", help = "json report", required = True, type=str)
    parser.add_argument("-n", help = "number of randomly selected genomes", default = 1, type = int)
    parser.add_argument("-v", "--view-dir", help="directory where views results for single species are stored", required = True, type=str)
    parser.add_argument("-t", "--target", help = "regex for databases to target (ex : crispr_rc03_v[0-10])", required = True)
    parser.add_argument("-f", "--fasta-dir", help = "fasta directory", required = True, type=str)
    parser.add_argument("-r", "--results", metavar = "<directory>", help = "directory to write individual results for each genome", required = True, type=str)


    return parser.parse_args()

def storeAlreadyChecked(log):
    stored = set()
    with open(log) as f:
        f.readline()
        for l in f:
            stored.add(l.split("\t")[0])
    return stored

def call_per_specie(uuid:str, log_dic, db, fasta_dir, view_dir, target, results_dir):
    results_file = f"{results_dir}/{uuid}_consistency.json"

    if not os.path.isfile(f"{view_dir}/{uuid}.json"):
        logging.info(f"== Create {uuid} view")
        db_manager_cmd = ["node", f"{DB_MANAGER_PATH}/build/index.js", "--config", f"{DB_MANAGER_PATH}/config.json", "--target", target, "--find", uuid, "--output_dir", view_dir]
        subprocess.run(db_manager_cmd, check = True)

    logging.info("View constructed")

    logging.info("Check consistency")
    res,time_spend = process_sequences(uuid, db, fasta_dir, view_dir, results_file)
    log_dic[uuid]["time"] = time_spend
    log_dic[uuid]["results_date"] = str(date.today())
    log_dic[uuid]["results"] = results_file
    if res : 
        log_dic[uuid]["checking_results"] = "ok"
    else:
        log_dic[uuid]["checking_results"] = "error"

    json.dump(log_dic, open(ARGS.log, "w"))

    logging.info(f"Results write to {results_file}")
    logging.info(f"{ARGS.log} updated")

    
    
        

if __name__ == "__main__":
    ARGS = args_gestion()

    db = dbManager.DatabaseManager(ARGS.config)

    already_selected = {}
    if os.path.isfile(ARGS.log):
        already_selected = json.load(open(ARGS.log))

    ##Sampling

    target_genomes = db.genomedb.all_ids.difference(set(already_selected.keys()))

    print(f"{len(target_genomes)} genomes to target")

    randoms = random.sample(target_genomes, ARGS.n)

    new_selection = {uuid : {"selection_date" : str(date.today()), "checking_results": "waiting", "results_date": "", "time" : 0, "results":""} for uuid in randoms}

    final_selection = {**already_selected, **new_selection}
    unchecked_selection = {k:v for k,v in final_selection.items() if v["checking_results"] == "waiting"} #uuid to process

    ##Construct views

    if not os.path.isdir(ARGS.view_dir):
        logging.info(f"== Create view directory {ARGS.view_dir}")
        os.mkdir(ARGS.view_dir)

    if not os.path.isdir(ARGS.results):
        logging.info(f"== Create individual results directory {ARGS.results}")
        os.mkdir(ARGS.results)

    for uuid in unchecked_selection:
        logging.info(f"== Work with {uuid}")
        call_per_specie(uuid, final_selection, db, ARGS.fasta_dir, ARGS.view_dir, ARGS.target, ARGS.results)
        #except Exception as e:
        #    logging.error(f"Error for {uuid} : {e}\nGo to next uuid.")

        






    


