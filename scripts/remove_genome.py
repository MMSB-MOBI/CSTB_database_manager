import argparse, logging, sys
import CSTB_database_manager.databaseManager as dbManager
import CSTB_database_manager.utils.error as error
from CSTB_core.utils.io import tsvReader, zExists

logging.basicConfig(level = logging.INFO, format='%(levelname)s\t%(filename)s:%(lineno)s\t%(message)s', stream=sys.stdout)

def args_gestion():
    parser = argparse.ArgumentParser(description = "Remove genome from database (except motif collection) from metadata list or ids list")
    parser.add_argument("-l", "--metadata_list", metavar = "<file>", help = "tsv file with metadata for a list of genomes (columns in this order : fasta taxid name gcf accession)", required = True)
    parser.add_argument("-c", "--config", metavar = "<json file>", help = "database config file", required = True)
    parser.add_argument("-f", "--fasta_dir", metavar = "<dir>", help = "Directory where fasta file are stored", required = True)
    return parser.parse_args()

if __name__ == "__main__":
    ARGS = args_gestion()

    db = dbManager.DatabaseManager(ARGS.config)

    if ARGS.metadata_list: 
        for (fasta, name, taxid, gcf, acc) in tsvReader(ARGS.metadata_list):
            fasta_path = ARGS.fasta_dir + "/" + fasta
            if not zExists(fasta_path):
                raise ValueError(f'No fasta file at {fasta_path}')
            try : 
                deleted_id = db.removeGenomeFromFasta(fasta_path, name, taxid, gcf, acc)
            except error.ConsistencyError as e:  
                logging.error(f"Can't remove your genome because of ConsistencyError\nReason : \n{e}")

            logging.info(f"{deleted_id} successfully deleted from genome and taxon collection")
    
    #NEED TO ADD DELETION IN INDEX AND BLAST 



