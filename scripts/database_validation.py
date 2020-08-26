#!/software/mobi/crispr-manager/2.0.0/bin/python

import argparse
import CSTB_database_manager.databaseManager as dbManager

def args_gestion():
    parser = argparse.ArgumentParser(description="Get some stats for crispr database.")
    
    parser.add_argument("--config", help = "json config file (see config.json for format)", required = True,  type=str, metavar = "FILE")
    parser.add_argument("--map", help = "rules to dispatch sgRNA to database endpoints. Here is used to have the list of volumes.", required = True, metavar ="FILE", type=str)

    return parser.parse_args()

if __name__ == "__main__":
    ARGS = args_gestion()
    db = dbManager.DatabaseManager(ARGS.config)

    motif_genomes, similarity = db.getEncodedSgrnaWithGenomes()
