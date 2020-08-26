#!/software/mobi/crispr-manager/2.0.0/bin/python
import matplotlib.pyplot as plt

import logging, sys, argparse
logging.basicConfig(level = logging.INFO, format='%(levelname)s\t%(filename)s:%(lineno)s\t%(message)s', stream=sys.stdout)
import CSTB_database_manager.databaseManager as dbManager

def args_gestion():
    parser = argparse.ArgumentParser(description="Get some stats for crispr database.")
    
    parser.add_argument("--config", help = "json config file (see config.json for format)", required = True,  type=str, metavar = "FILE")
    parser.add_argument("--map", help = "rules to dispatch sgRNA to database endpoints. Here is used to have the list of volumes.", required = True, metavar ="FILE", type=str)

    return parser.parse_args()

def plot_entries_per_volume_distrib(entries_per_volume, plot_file):
    numbers = list(entries_per_volume.values())
    fig, ax = plt.subplots()
    ax.set_title(f"Number of sgRNA per volume")
    ax.set_xlabel("Number of sgRNA")
    ax.set_ylabel("Count")
    ax.hist(numbers)
    fig.savefig(f"{plot_file}.png", format = "png")
    logging.info(f"Number of sgRNA per volume saved to {plot_file}.png")


if __name__ == "__main__":
    ARGS = args_gestion()
    db = dbManager.DatabaseManager(ARGS.config, ARGS.map)
    
    fasta_number = db.genomedb.number_of_entries
    taxon_number = db.taxondb.number_of_entries
    entries_per_volume = db.motifsdb.entries_per_volume

    o = open("general_stats.txt", "w")
    o.write(f"Number of fasta : {fasta_number}\nNumber of represented taxons : {taxon_number}")
    o.close()

    logging.info("General stats are written in general_stats.txt")

    plot_entries_per_volume_distrib(entries_per_volume, "entries_per_volume")
    

