import argparse, sys, logging
import CSTB_database_manager.databaseManager as dbManager
logging.basicConfig(level = logging.INFO, format='%(levelname)s\t%(filename)s:%(lineno)s\t%(message)s')

def args_gestion():
    parser = argparse.ArgumentParser(description = "Check consistency between motifs collection and genome collection")
    parser.add_argument("-m", "--motif_ranks", metavar = '<json file>', help = "json file from ms-db-manager that stores organisms ids and number of occurences. Required if db1 or db2 is motif")
    parser.add_argument("-c", "--config", metavar = "<json file>", help = "json database config file", required = True)
    parser.add_argument("--metadata_out", metavar = "<tsv file>", help = "Write metadata for ids found in genome and not in motif if exists. Required if db1 or db2 is genome")
    parser.add_argument("--db1", metavar = "<database>", help = "First database to check : genome | motif | blast | index", required = True)
    parser.add_argument("--db2", metavar = "<database>", help = "Second database to check : genome | motif | blast | index", required = True)

    args = parser.parse_args()
    #Check arguments
    list_db = ["genome", "motif", "blast", "index"]
    has_to_quit = False
    if not args.db1 in list_db:
        logging.error("--db1 is not valid, must be genome | motif | blast | index")
        has_to_quit = True
    if not args.db2 in list_db: 
        logging.error("--db2 is not valid, must be genome | motif | blast | index")
        has_to_quit = True
    if args.db1 == args.db2 : 
        logging.error("--db1 and --db2 are the same")
        has_to_quit = True
    if (args.db1 == "motif" or args.db2 == "motif") and not args.motif_ranks:
        logging.error("You need to provide -m/--motif_ranks because you want to check in motif database")
        has_to_quit = True
    if (args.db1 == "genome" or args.db2 == "genome") and not args.metadata_out:
        logging.error("You need to provide -metadata_out because you want to check in genome database")
        has_to_quit = True
    
    if has_to_quit:
        return

    return args

if __name__ == "__main__":
    ARGS = args_gestion()
    if not ARGS:
        exit() 

    db = dbManager.DatabaseManager(ARGS.config)
    in_db1, in_db2 = db.checkConsistency(ARGS.db1, ARGS.db2, ARGS.motif_ranks, ARGS.metadata_out)

    to_print_in_db1 = "\n".join(in_db1)
    to_print_in_db2 = "\n".join(in_db2)

    print(f'#Present in {ARGS.db1} collection and not in {ARGS.db2} collection ({len(in_db1)})\n{to_print_in_db1}\n#Present in {ARGS.db2} collection and not in {ARGS.db1} collection ({len(in_db2)})\n{to_print_in_db2}')
    if ARGS.metadata_out:
        print(f"Metadata in {ARGS.metadata_out}")


