import argparse, sys, logging
import CSTB_database_manager.databaseManager as dbManager
logging.basicConfig(level = logging.INFO, format='%(levelname)s\t%(filename)s:%(lineno)s\t%(message)s')

def args_gestion():
    parser = argparse.ArgumentParser(description = "Check consistency between motifs collection and genome collection")
    parser.add_argument("-m", "--motif_ranks", metavar = '<json file>', help = "json file from ms-db-manager that stores organisms ids and number of occurences", required = True)
    parser.add_argument("-c", "--config", metavar = "<json file>", help = "json database config file", required = True)
    parser.add_argument("--metadata_out", metavar = "<tsv file>", help = "Write metadata for ids found in genome and not in motif if exists", required = True)
    return parser.parse_args()

if __name__ == "__main__":
    ARGS = args_gestion()
    db = dbManager.DatabaseManager(ARGS.config)
    motifs_not_genome, genome_not_motifs = db.checkConsistency(ARGS.motif_ranks, ARGS.metadata_out)
    to_print_motifs_not_genome = "\n".join(motifs_not_genome)
    to_print_genome_not_motifs = "\n".join(genome_not_motifs)

    print(f'#Present in motif collection and not in genome collection\n{to_print_motifs_not_genome}\n#Present in genome collection and not in motif collection\n{to_print_genome_not_motifs}')
    if genome_not_motifs:
        print(f"Metadata in {ARGS.metadata_out}")


