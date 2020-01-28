"""Test create tree

Usage:
    use.py --config <conf>

Options:
    -h --help
    --config <conf>  json config file (see config.json for format)

"""

import sys
sys.path.append("/home/chilpert/Dev/CSTB_database_manager/lib")
import CSTB_database_manager.databaseManager as dbManager
from docopt import docopt

if __name__ == "__main__":
    ARGS = docopt(__doc__, version="Use cases")

    db = dbManager.DatabaseManager(ARGS["--config"])

    db.createTree()