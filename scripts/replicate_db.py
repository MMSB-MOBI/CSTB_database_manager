"""Replicate a database

Usage:
    replicate_database.py tree --config <conf> --target <str>
    replicate_database.py taxon --config <conf> --target <str>
    replicate_database.py genome --config <conf> --target <str>

Options:
    -h --help
    --config <conf>  json config file (see config.json for format)
    --target <str> name of replicated database

"""

import sys
sys.path.append("/home/chilpert/Dev/CSTB_database_manager/lib")
import CSTB_database_manager.databaseManager as dbManager
from docopt import docopt

if __name__ == "__main__":
    ARGS = docopt(__doc__, version="Use cases")

    #if ARGS["source"] not in ["tree", "taxon", "genome"]:
    #    raise Exception(f"source must be tree, taxon or genome")

    db = dbManager.DatabaseManager(ARGS["--config"])

    if ARGS["tree"]:
        database = db.treedb
    elif ARGS["taxon"]:
        database = db.taxondb
    elif ARGS["genome"]: 
        database = db.genomedb

    database.replicate(ARGS["--target"])

    



