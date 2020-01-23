"""Add genomes to taxon and genome databases

Usage:
    add_genome.py --config <conf> --genomes <genome_list> --location <fasta_folder> [--map <volume_mapper>] [ --min <start_index> --max <stop_index> --cache <pickle_cache> ] [ --debug ] [ --size <batch_size> ]

Options:
    -h --help
    --config <conf>  json config file (see config.json for format)
    --genomes <genome_list> tsv file with list of genomes (columns in this order : fasta taxid name gcf accession)
    --location <fasta_folder> path to folder containing referenced fasta in tsv file
    --map <volume_mapper> rules to dispatch sgRNA to database endpoints. MANDATORY for sgRNA motif insertions
    --min <start_index> position to read from (included) in the tsv file (header line does not count)
    --max <stop_index>  position to read to   (included) in the tsv file (header line does not count)
    --cache <pickle_cache>  define a folder to dump the sgnRNA locations of each provided genome
    --size <batch_size>  Maximal number of keys in a couchDB volume collection insert (default = 10000)
    --debug  Set debug mode ON

"""

import sys, os
import CSTB_database_manager.databaseManager as dbManager
from docopt import docopt
from CSTB_database_manager.utils.io import tsvReader

if __name__ == "__main__":
    ARGS = docopt(__doc__, version="1.0.0")

    print(ARGS)

    db = dbManager.DatabaseManager(ARGS["--config"])
   
    x = int(ARGS["--min"]) if not ARGS["--min"] is None  else 0
    y = int(ARGS["--max"]) if not ARGS["--max"] is None else len([ _ for _ in tsvReader(ARGS["--genomes"])])
    cacheLocation = ARGS["--cache"] if "--cache" in ARGS else None
    if ARGS["--debug"]:
        db.setDebugMode()

    fastaFileList = []
    for (fasta, name, taxid, gcf, acc) in tsvReader(ARGS["--genomes"], x, y):
        fastaFileList.append(ARGS["--location"] + '/' + fasta)
        if not os.path.isfile(fastaFileList[-1]):
            raise ValueError(f"No fasta file at {fastaFileList[-1]}")
        
        print(f"db.AddGenome({fastaFileList[-1]}, {name}, {taxid}, {gcf}, {acc})")           
        db.addGenome(fastaFileList[-1], name, taxid, gcf, acc)
    
    if ARGS["--map"]: 
        db.setMotifAgent(ARGS["--map"])
        print(f"Proceeding to the db.AddMotifs of {len(fastaFileList)} fasta")  
        bSize = int(ARGS["--size"]) if ARGS["--size"] else 10000                
        db.addFastaMotifs(fastaFileList, bSize , cacheLocation)


"""
for i in `seq 0 255`
do
curl -X PUT http://wh_agent:couch@127.0.0.1:5984/crispr_rc20_beta_v$i
done
"""