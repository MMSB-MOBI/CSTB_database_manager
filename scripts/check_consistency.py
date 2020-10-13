#!/software/mobi/crispr-manager/2.0.0/bin/python

import argparse, sys, logging, json, time
import CSTB_database_manager.databaseManager as dbManager
import motif_broker_request.request as mb_request
from CSTB_core.engine.word_detect import sgRNAfastaSearch

logging.basicConfig(level = logging.INFO, format='%(levelname)s\t%(filename)s:%(lineno)s\t%(message)s')

def load_sgrnas_from_view(view_path):
    view = json.load(open(view_path))
    sgrnas = []
    for vol in view["find"]:
        sgrnas = sgrnas + vol["view"]["sgrnas"]
    return sgrnas

def motif_broker_filter_genomes(mb_res, **kwargs):
    if not "genomes" in kwargs:
        raise Exception("you must provide 'genomes' argument to get function for filter_genomes function")
    genomes = kwargs["genomes"]

    filtered_results = {}
    for sgrna in mb_res: 
        added = False 
        for org in mb_res[sgrna]:
            if org in genomes:
                if added:
                    filtered_results[sgrna][org] = mb_res[sgrna][org]
                else:
                    filtered_results[sgrna] = {org : mb_res[sgrna][org]}
    
    return filtered_results

def args_gestion():
    parser = argparse.ArgumentParser(description = "Check consistency in crispr database")

    subparsers = parser.add_subparsers(dest='subparser_name')
    parser_org = subparsers.add_parser('genomes', help='Check consistency between different collections, regarding to genomes presence')
    parser_org.add_argument("-c", "--config", metavar = "<json file>", help = "json database config file", required = True)

    parser_org.add_argument("-m", "--motif_ranks", metavar = '<json file>', help = "json file from ms-db-manager that stores organisms ids and number of occurences. Required if db1 or db2 is motif")
    
    parser_org.add_argument("--metadata_out", metavar = "<tsv file>", help = "Write metadata for ids found in genome and not in motif if exists. Required if db1 or db2 is genome")
    parser_org.add_argument("--db1", metavar = "<database>", help = "First database to check : genome | motif | blast | index", required = True)
    parser_org.add_argument("--db2", metavar = "<database>", help = "Second database to check : genome | motif | blast | index", required = True)

    parser_seq = subparsers.add_parser('sequences', help="Check consistency for one organism between motifs and fasta content")
    parser_seq.add_argument("-u", "--uuid", help = "genome uuid", required = True, type=str)
    parser_seq.add_argument("-v", "--view-dir", help = "directory with single specie views results stored", required = True, type=str)
    parser_seq.add_argument("-f", "--fasta-dir", help = "fasta directory", required = True, type=str)
    parser_seq.add_argument("-c", "--config", metavar = "<json file>", help = "json database config file", required = True)
    parser_seq.add_argument("-r", "--results", metavar = "<json file>", help = "where to write results as json", required = True, type=str)


    args = parser.parse_args()
    if args.subparser_name == "genomes" :
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
    
    elif args.subparser_name == "sequences":
        pass

    else:
        parser.print_help()
        logging.error("Choose between sequences or genomes")
        return

    return args

if __name__ == "__main__":
    ARGS = args_gestion()
    if not ARGS:
        exit() 

    start = time.time()

    db = dbManager.DatabaseManager(ARGS.config)

    if ARGS.subparser_name == "genomes":
        in_db1, in_db2 = db.checkConsistency(ARGS.db1, ARGS.db2, ARGS.motif_ranks, ARGS.metadata_out)

        to_print_in_db1 = "\n".join(in_db1)
        to_print_in_db2 = "\n".join(in_db2)

        print(f'#Present in {ARGS.db1} collection and not in {ARGS.db2} collection ({len(in_db1)})\n{to_print_in_db1}\n#Present in {ARGS.db2} collection and not in {ARGS.db1} collection ({len(in_db2)})\n{to_print_in_db2}')
        if ARGS.metadata_out:
            print(f"Metadata in {ARGS.metadata_out}")
    
    elif ARGS.subparser_name == "sequences":

        #Get fasta name from couch
        #Get dic from fasta

        logging.info("Get fasta path from genome collection...")
        genome_doc = db.genomedb.get_from_uuid(ARGS.uuid)
        if not genome_doc : 
            raise Exception(f"{ARGS.uuid} doesn't exist in genome collection {db.genomedb.db_name}")
        fasta = genome_doc["fasta_name"]
        #Check fastahash is good ?? 

        logging.info("Search sgRNA in fasta...")
        fasta_dic = sgRNAfastaSearch(ARGS.fasta_dir + "/" + fasta, ARGS.uuid)

        #Get sgRNA from view
        logging.info("Load species sgRNAs...")
        all_sgrnas = load_sgrnas_from_view(f"{ARGS.view_dir}/{ARGS.uuid}.json")
        logging.info(f"{len(all_sgrnas)} sgRNAs loaded") 

        #Interrogate motif-broker with genome filtering
        logging.info("Interrogate motif-broker...")
        mb_result = mb_request.get(all_sgrnas, filter_predicate = motif_broker_filter_genomes, genomes = [ARGS.uuid])

        fasta_sequences = set(fasta_dic.keys())
        mb_sequences = set(mb_result.keys())

        fasta_not_in_mb = fasta_sequences.difference(mb_sequences)
        mb_not_in_fasta = mb_sequences.difference(fasta_sequences)

        results_json = {"fasta_not_collection": list(fasta_not_in_mb), "collection_not_fasta": list(mb_not_in_fasta)}
        print("In fasta not in collection :", len(fasta_not_in_mb), "sequences")
        print("In collection not in fasta :", len(mb_not_in_fasta), "sequences")

        json.dump(results_json, open(ARGS.results, "w"))



    logging.info(f"END in {time.time() - start}")



