"""Use cases for add genomes in CSTB database

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

    # Add completely new genome with taxid
    print("# ADD NEW V.CHOLERAE")
    db.addGenome('../data_tests/vibrio_cholerae_10k.fna', "Vibrio cholerae O1 biovar El Tor str. N16961", 243277, gcf = "my_gcf", acc = "my_acc")
    print()

    #Add same genome
    print("# ADD SAME V.CHOLERAE")
    db.addGenome('../data_tests/vibrio_cholerae_10k.fna', "Vibrio cholerae O1 biovar El Tor str. N16961", 243277, gcf = "my_gcf", acc = "my_acc")
    print()

    print("# ADD WITH OTHER GCF AND ACC")
    db.addGenome('../data_tests/vibrio_cholerae_10k.fna', "Vibrio cholerae O1 biovar El Tor str. N16961", 243277, gcf = "abc", acc = "def")
    print() 

    #Add an other
    print("# ADD E.COLI")
    db.addGenome("../data_tests/ecoli_O157_1_10k.fna", "Escherichia coli O157:H7 str. EDL933", 155864)
    print()

    #Add other version
    print("# ADD OTHER VERSION OF E.COLI")
    db.addGenome("../data_tests/ecoli_O157_2_10k.fna", "Escherichia coli O157:H7 str. EDL933", 155864)
    print()

    #Reinsert old version
    print("# REINSERT FIRST ECOLI")
    db.addGenome("../data_tests/ecoli_O157_1_10k.fna", "Escherichia coli O157:H7 str. EDL933", 155864)
    print()

    #Insert already existed genome with other taxon name
    print("# INSERT ECOLI WITH OTHER NAME")
    db.addGenome("../data_tests/ecoli_O157_1_10k.fna", "Pouet", 155864)
    print()

    #Insert already existed genome with other taxid
    print("# INSERT ECOLI WITH OTHER TAXID")
    db.addGenome("../data_tests/ecoli_O157_1_10k.fna", "Escherichia coli O157:H7 str. EDL933", 2)
    print()

    #Insert already existed genome with other name and other taxid
    print("# INSERT ECOLI WITH OTHER NAME AND OTHER TAXID")
    db.addGenome("../data_tests/ecoli_O157_1_10k.fna", "Pouet", 5874)
    print()

    #Insert already existed genome with taxid that exists 
    print("# INSERT ECOLI WITH V.CHOLERAE TAXON")
    db.addGenome('../data_tests/ecoli_O157_1_10k.fna', "Vibrio cholerae O1 biovar El Tor str. N16961", 243277)
    print()


    #Insert plasmid with no taxid
    print("# INSERT PLASMID WITH NO TAXID")
    db.addGenome("../data_tests/pOXA-48.fna", "pOXA-48")
    print()

    #Insert same
    print("# INSERT SAME PLASMID")
    db.addGenome("../data_tests/pOXA-48.fna", "pOXA-48")
    print()

    #Insert under an other name
    print("# INSERT PLASMID WITH AN OTHER NAME")
    db.addGenome("../data_tests/pOXA-48.fna", "pOXA")
    print()

    #Insert suddenly with taxid 
    print("# INSERT PLASMID SUDDENLY WITH TAXID")
    db.addGenome("../data_tests/pOXA-48.fna", "pOXA-48", 12)
    print()

    #Insert new genome with bad taxon
    print("# INSERT GENOME WITH BAD TAXON")
    db.addGenome("../data_tests/pseudo_syringae_10k.fna", "pouet", 243277, acc = "NC_004578.1")