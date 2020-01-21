"""Add genomes to taxon and genome databases

Usage:
    add_genome.py --config <conf> --genomes <genome_list>

Options:
    -h --help
    --config <conf>  json config file (see config.json for format)
    --genomes <genome_list> tsv file with list of genomes (columns in this order : fasta taxid name gcf accession)

"""

import sys
sys.path.append("/home/chilpert/Dev/CSTB_database_manager/lib")
import CSTB_database_manager.databaseManager as dbManager
from docopt import docopt

if __name__ == "__main__":
    ARGS = docopt(__doc__, version="1.0.0")

    db = dbManager.DatabaseManager(ARGS["--config"])
    i=0
    with open(ARGS["--genomes"]) as f : 
        f.readline()
        for l in f: 
            i += 1 
            l_split = l.split("\t")
            fasta = l_split[0]
            if not fasta:
                raise FormatError("genomes list, first column (fasta) is empty")
            taxid = l_split[1]
            if not taxid:
                taxid = None
            else:
                taxid = int(taxid)
            name = l_split[2]
            if not name:
                raise FormatError("genomes list, third column (name) is empty")
            gcf = l_split[3]
            if not gcf:
                gcf = None
            acc = l_split[4]
            if not acc or acc == "\n":
                acc = None
            
            db.addGenome(fasta, name, taxid, gcf, acc)
            print()