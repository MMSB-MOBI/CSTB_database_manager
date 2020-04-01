# CSTB_database_manager

CSTB database is a collection of 3 types of couchDB databases (taxon_db, genome_db, tree_db and crispr_motif) and some local index files in the system.

## [Detailed documentation](https://mmsb-mobi.github.io/CSTB_database_manager/)

## Database structure
To initialize CSTB database via databaseManager, you must provide a json config file formatted like : 


### Taxon and Genome databases
Taxon and genome databases are linked, modification in one triggers modification in the other.

#### Taxon database
Taxon database is a collection of of documents : 
```
{
  "_id": "32cd400c3f5997cfdd3abd290e0bbebc",
  "_rev": "1-816c9471f38acea4005713248a96ff90",
  "name": "Vibrio cholerae O1 biovar El Tor str. N16961",
  "taxid": 243277,
  "current": "32cd400c3f5997cfdd3abd290e0bb43f",
  "genomeColl": [
    "32cd400c3f5997cfdd3abd290e0bb43f"
  ]
}
```
`genomeColl` and `current` are references to Genome document. `taxid` can be `null`

#### Genome database
Genome database is a collection of documents : 
```
{
  "_id": "32cd400c3f5997cfdd3abd290e0bb43f",
  "_rev": "1-e6b9861dc339a082d9ca6a1a06fe6c8d",
  "fasta_md5": "6399150c8cbed82ed0f50b8d6b09f8bc",
  "taxon": "32cd400c3f5997cfdd3abd290e0bbebc",
  "gcf_assembly": "my_gcf", 
  "accession_number" : "my_acc"
}
```
`taxon` are reference to Taxon document. `gcf_assembly` and `accession_number` can be `null`

### Tree database
Tree database just store taxonomic tree with correct format to be display by jquery. Example : 
```
{
  "_id": "maxi_tree",
  "_rev": "2-ded77c3388cd803d160d8e2a0df82481",
  "date": "28/01/2020 15:45:12",
  "tree": {
    "text": "root",
    "children": [
      {
        "text": "Myxococcales",
        "children": [
          {
            "text": "Cystobacterineae",
            "children": [
              {
                "text": "Archangium gephyra"
              },
              {
                "text": "Myxococcus macrosporus"
              }
            ]
          },
          {
            "text": "Chondromyces crocatus"
          }
        ]
      }
    ]
  }
}
```

### Motifs databases

### Blast database

### Index database

## Add a new Entry

To add new entry, you will need config file (config.json) and tsv file (genomes.tsv) with list of genomes. To add in motifs databases, you will need a mapping file.

**config.json** is a json file : 
```
{
    "url": "http://localhost:5984",
    "user": "couch_agent",
    "password": "couch",
    "taxondb_name": "taxon_db",
    "genomedb_name": "genome_db",
    "treedb_name" : "tree_db",
    "blastdb_path" : "/mobi/group/databases/crispr/crispr_rc02/blast"
}
```

**genomes.tsv**, 5 first columns are mandatory, next columns are optional : 
```
#fasta	taxid	name	gcf	accession	ftp
GCF_000010525.1_ASM1052v1_genomic.fna 438753  Azorhizobium caulinodans ORS 571  GCF_000010525.1	-	ftp://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/000/010/525/GCF_000010525.1_ASM1052v1/GCF_000010525.1_ASM1052v1_genomic.fna.gz
GCF_000007365.1_ASM736v1_genomic.fna	198804	Buchnera aphidicola str. Sg (Schizaphis graminum)	GCF_000007365.1	-	ftp://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/000/007/365/GCF_000007365.1_ASM736v1/GCF_000007365.1_ASM736v1_genomic.fna.gz
...
```

**mapping.json**, json file that contains regex mapping rules for motifs databases : 
```
{
  "^AAAA[ACGT]{19}$": "crispr_rc02_v0",
  "^AAAT[ACGT]{19}$": "crispr_rc02_v1",
  "^AAAC[ACGT]{19}$": "crispr_rc02_v2"
  ...
}
```

### Usage

```
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
```

### Examples

#### Add 10 first genomes in genome and taxon databases
```
python add_genome.py --config config.json --genomes genomes.tsv --location <fasta_folder> --min 0 --max 9
```

#### Add 10 first genomes in genome, taxon and motifs/index databases 
* With pickle cache
```
python add_genome.py --config config.json --genomes genomes.tsv --location <fasta_folder> --min 0 --max 9 --map mapping.json --index <index_file_dump_loc> --cache <cache_directory>
```
* Without cache
```
python add_genome.py --config config.json --genomes genomes.tsv --location <fasta_folder> --min 0 --max 9 --map mapping.json --index <index_file_dump_loc>
```

#### Add genomes to blast database
 ```
python add_genome.py --config config.json --genomes genomes.tsv --location <fasta_folder> --blast
```

#### Add tree to tree databases from taxon informations stored in taxon database
```
python add_genome.py --config config.json --genomes genomes.tsv --location <fasta_folder> --tree
```

#### Add all genomes in all databases
```
python add_genome.py --config config.json --genomes genomes.tsv --location <fasta_folder> --map mapping.json --index <index_file_dump_loc> --blast --tree
```

## Check consistency in database

First, you need to get summary of motif database content by using [ms-db-manager
](https://github.com/glaunay/ms-db-manager). 

We advice to first set the views if not already done : 
```
cd ms-db-manager/build
node index.js --target 'crispr_rc01_v[0-255]' --design ../views/byOrganism.json --config ../config.json
```

Then rank the data : 
```
node index.js --target 'crispr_rc021_v[0-255] --rank ../crispr_rc021_species.json --config ../config.json
```
Ranked json file will be used for checking consistency. 

You can now check consistency with : 
```
python scripts/check_consistency.py -c config.json -m crispr_rc021_species.json
```

The script write results in standard output and will display ids present in motif collection and not in genome collection and opposite. Example of result : 
```
#Present in motif collection and not in genome collection

#Present in genome collection and not in motif collection
e7bdf2793c3942870a9f84806a6846be
e7bdf2793c3942870a9f84806a68359d
e7bdf2793c3942870a9f84806a681b60
e7bdf2793c3942870a9f84806a684b19
e7bdf2793c3942870a9f84806a6800cc
```

If you have unconsistency in the database, it's your job to fix problem until consistency.

**Example of cases** 
* I have ids present in genome collection and not in motifs collection : 
  * I want to add this genomes in motif collection : Re-use the add_genome.py script for this list of genomes, if fasta and taxon are the same, the same id will be reused in motif collection. If fasta is different, it will be considered as new and current version of the taxon. You can delete the old version if you want. 
  * I want to delete this genomes from genome collection : Use remove_genome.py, see Remove genome section in this readme. 

* I have ids present in motifs collection and not in genome collection : 
    * I want to delete this genomes from motif collection : use [ms-db-manager](https://github.com/glaunay/ms-db-manager)

    * I want to add this genomes in genome collection : for now it's not possible to add the genomes and conserve the same ids. We advice to delete from motif collection and re-add, a new id will be used.

## Remove genome

You can **delete a genome from genome and taxon collections** the same way as adding a genome. 

```
usage: remove_genome.py [-h] -l <file> -c <json file> -f <dir>
  -h, --help            show this help message and exit
  -l <file>, --metadata_list <file>
                        tsv file with metadata for a list of genomes (columns in this order : fasta taxid name gcf accession)
  -c <json file>, --config <json file>
                        database config file
  -f <dir>, --fasta_dir <dir>
                        Directory where fasta file are stored
```

Then **delete the motifs** corresponding to genome with [ms-db-manager
](https://github.com/glaunay/ms-db-manager) (part Delete all sgRNAs relative to a particular specie)

Don't forget to **delete index files** in file system

TO IMPLEMENT : blast ?? 










