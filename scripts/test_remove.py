"""Test remove

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

    print("# REMOVE GENOME WITH ONE VERSION")
    db.removeGenome('/mnt/arwen/mobi/group/databases/crispr/crispr_rc01/fasta/GCF_000006845.1_ASM684v1_genomic.fna')
    print()

    print("# REMOVE GENOME THAT DOESN'T EXIST")
    db.removeGenome('../data_tests/ecoli_O157_1_10k.fna')
    print()

    try:
        db.removeGenome("/mnt/arwen/mobi/group/databases/crispr/crispr_rc01/fasta/GCF_001027285.1_ASM102728v1_genomic.fna")
    except Exception as e:
        print(e) 
        

    db.removeGenome("/mnt/arwen/mobi/group/databases/crispr/crispr_rc01/fasta/")
