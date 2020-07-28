#!/software/mobi/crispr-manager/2.0.0/bin/python

"""Remove genome from database (except motif collection)

Usage:
    remove_genome.py --config <conf> --genomes <genome_list> --location <fasta_folder> [ --min <start_index> --max <stop_index> ] [ --tree ] [ --blast ]

Options:
    -h --help
    --config <conf>  json config file (see config.json for format)
    --genomes <genome_list> tsv file with list of genomes to remove (columns in this order : fasta taxid name gcf accession)
    --location <fasta_folder> path to folder containing referenced fasta in tsv file
    --min <start_index> position to read from (included) in the tsv file (header line does not count)
    --max <stop_index>  position to read to   (included) in the tsv file (header line does not count)
    --tree  Create taxonomic tree after deletion
    --blast  Remove from blast

"""

import logging, sys
from docopt import docopt
import CSTB_database_manager.databaseManager as dbManager
import CSTB_database_manager.utils.error as error
from CSTB_core.utils.io import tsvReader, zExists

logging.basicConfig(level = logging.INFO, format='%(levelname)s\t%(filename)s:%(lineno)s\t%(message)s', stream=sys.stdout)


if __name__ == "__main__":
    ARGS = docopt(__doc__, version="1.0.0")

    db = dbManager.DatabaseManager(ARGS["--config"])

    x = int(ARGS["--min"]) if not ARGS["--min"] is None  else 0
    y = int(ARGS["--max"]) if not ARGS["--max"] is None else len([ _ for _ in tsvReader(ARGS["--genomes"])])

    fastaFileList = []
    for (fasta, name, taxid, gcf, acc) in tsvReader(ARGS["--genomes"], x, y):
        fasta_path = ARGS["--location"]  + "/" + fasta
        if not zExists(fasta_path):
            logging.warn(f'No fasta file at {fasta_path}')
            continue
        fastaFileList.append(fasta_path)
        
    #First remove from blast, else it will be impossible to retrieve genome id
    
    if ARGS["--blast"]:
        logging.info("# Remove from Blast")
        db.removeFromBlast(fastaFileList)    

    logging.info("# Remove from Genome and Taxon")
    o = open("removed_genomes.log", "w")
    for (fasta, name, taxid, gcf, acc) in tsvReader(ARGS["--genomes"], x, y):
        fasta_path = ARGS["--location"]  + "/" + fasta
        try : 
            deleted_id = db.removeGenomeFromGenomeAndTaxon(fasta_path, name, taxid, gcf, acc)
        except error.ConsistencyError as e:  
            logging.error(f"Can't remove your genome because of ConsistencyError\nReason : \n{e}")
        if deleted_id:
            o.write(deleted_id  + "\n")
    o.close()

    logging.info("List of deleted ids in removed_genomes.log")

    if ARGS["--tree"]:
        db.createTree()
    
    



