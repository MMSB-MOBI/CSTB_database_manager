"""Add sgRNA from fasta to motif databases

Usage:
    add_sgRNA.py --config <conf> --genomes <genome_list> --location <fasta_folder>

Options:
    -h --help
    --config <conf>  json config file (see config.json for format)
    --genomes <genome_list> tsv file with list of genomes (columns in this order : fasta taxid name gcf accession)
    --location <fasta_folder> path to folder containing referenced fasta in tsv file

"""

import sys, os
from docopt import docopt

import CSTB_database_manager.databaseManager as dbManager
from CSTB_database_manager.utils.io import tsvReader

if __name__ == "__main__":
    ARGS = docopt(__doc__, version="1.0.0")

    db = dbManager.DatabaseManager(ARGS["--config"])

    for (fasta, name, taxid, gcf, acc) in tsvReader(ARGS["--genomes"]):
        fastaFile = ARGS["--location"] + '/' + fasta
        if not os.path.isfile(fastaFile):
            raise ValueError(f"No fasta file at {fastaFile}")
        # Get UUID from genome datbase
        