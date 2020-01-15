"""Use cases for add genomes in CSTB database

Usage:
    use.py --config <conf>

Options:
    -h --help
    --config <conf>  json config file (see config.json for format)

"""

import sys
sys.path.append("/home/chilpert/Dev/CSTB_database_manager")
import CSTB_database_manager.databaseManager as dbManager
from docopt import docopt

if __name__ == "__main__":
    ARGS = docopt(__doc__, version="Use cases")

    db = dbManager.databaseManager(ARGS["--config"])

    # Add completely new genome with taxid
    db.addGenome('../data_tests/vibrio_cholerae_10k.fna', "Vibrio cholerae O1 biovar El Tor str. N16961", 243277)

    
    