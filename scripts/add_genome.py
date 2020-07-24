#!/software/mobi/crispr-manager/2.0.0/bin/python

"""Add genomes to taxon and genome databases

Usage:
    add_genome.py --config <conf> --genomes <genome_list> --location <fasta_folder> [--map <volume_mapper>] [--index <index_file_dump_loc>] [ --min <start_index> --max <stop_index> --cache <pickle_cache> ] [ --debug ] [ --size <batch_size> ] [ --tree ] [ --blast ]

Options:
    -h --help
    --config <conf>  json config file (see config.json for format)
    --genomes <genome_list> tsv file with list of genomes (columns in this order : fasta taxid name gcf accession)
    --location <fasta_folder> path to folder containing referenced fasta in tsv file
    --map <volume_mapper> rules to dispatch sgRNA to database endpoints. MANDATORY for sgRNA motif insertions
    --index <index_file_dump_loc> folder location to write integer encoded sgRNA genome content
    --min <start_index> position to read from (included) in the tsv file (header line does not count)
    --max <stop_index>  position to read to   (included) in the tsv file (header line does not count)
    --cache <pickle_cache>  define a folder to dump the sgnRNA locations of each provided genome
    --size <batch_size>  Maximal number of keys in a couchDB volume collection insert (default = 10000)
    --tree  Create taxonomic tree after insertion
    --debug  Set debug mode ON

"""

import sys, os
import logging
logging.basicConfig(level = logging.INFO, format='%(levelname)s\t%(filename)s:%(lineno)s\t%(message)s', stream=sys.stdout)
import CSTB_database_manager.databaseManager as dbManager
from docopt import docopt
from CSTB_core.utils.io import tsvReader, zExists

#from CSTB_database_manager.utils.io import fileHash as zHash

if __name__ == "__main__":
    ARGS = docopt(__doc__, version="1.0.0")

    db = dbManager.DatabaseManager(ARGS["--config"])
    
    x = int(ARGS["--min"]) if not ARGS["--min"] is None  else 0
    y = int(ARGS["--max"]) if not ARGS["--max"] is None else len([ _ for _ in tsvReader(ARGS["--genomes"])])
    bSize = int(ARGS["--size"]) if ARGS["--size"] else 10000                
       
    #cacheLocation = ARGS["--cache"] if "--cache" in ARGS else None
    #indexLocation = ARGS["--index"] if "--index" in ARGS else None
    cacheLocation = ARGS["--cache"]
    indexLocation = ARGS["--index"]

    if ARGS["--debug"]:
        db.setDebugMode()

    fastaFileList = []
    for (fasta, name, taxid, gcf, acc) in tsvReader(ARGS["--genomes"], x, y):
        
        fastaFileList.append(ARGS["--location"] + '/' + fasta)
        
        #print(zHash(fastaFileList[-1]))
        #exit(1)
        #logging.info(f"> {fastaFileList}")
        if not zExists(fastaFileList[-1]):
            raise ValueError(f'No fasta file at {fastaFileList[-1]}')
        
        logging.info(f"db.AddGenome({fastaFileList[-1]}, {name}, {taxid}, {gcf}, {acc})")           
        db.addGenome(fastaFileList[-1], name, taxid, gcf, acc)

    if ARGS["--map"]: 
        db.setMotifAgent(ARGS["--map"])
    
    if ARGS["--map"] or ARGS["--index"]:
        logging.info(f"Proceeding to the db.AddMotifs of {len(fastaFileList)} fasta")  
        db.addFastaMotifs(fastaFileList, bSize , indexLocation, cacheLocation)

    if ARGS["--blast"]: 
        db.addBlast(fastaFileList)

    if ARGS["--tree"]:
        db.createTree()

"""
for i in `seq 0 255`
do
curl -X PUT http://wh_agent:couch@127.0.0.1:5984/crispr_rc20_beta_v$i
done
"""
