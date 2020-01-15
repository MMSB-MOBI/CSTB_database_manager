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
    #db.addGenome('../data_tests/vibrio_cholerae_10k.fna', "Vibrio cholerae O1 biovar El Tor str. N16961", 243277)

    #Add same genome
    #db.addGenome('../data_tests/vibrio_cholerae_10k.fna', "Vibrio cholerae O1 biovar El Tor str. N16961", 243277)

    #Add an other
    db.addGenome("../data_tests/ecoli_O157_1_10k.fna", "Escherichia coli O157:H7 str. EDL933", 155864)

    #Add other version
    db.addGenome("../data_tests/ecoli_O157_2_10k.fna", "Escherichia coli O157:H7 str. EDL933", 155864)

