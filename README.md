# CSTB_database_manager

CSTB database is a collection of 4 types of couchDB databases (taxon_db, genome_db, tree_db and crispr_motif) and some local index files in the system.

## Summary
* [Database structure](#database-structure)
* [Add new genomes](#add-genome)
* [Check database consistency](#check-consistency)
* [Remove genomes](#remove-genome)
* [Replicate database](#replicate-database)
* [Update database](#update-database)

<p id="database-structure">

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

Motifs database is a collection of 256 volumes, each one identified by the 4 first letters of sgRNA (for example, v0 contains sgRNA that begins with AAAA, v1 contains sgRNA that begins with AAAT etc...). 
In each volume, there is one document per sgRNA. This document looks like this : 
```
{
  "_id": "AAAAAAAAAAAAAAAAAAAAAGG",
  "_rev": "4-93b04311132f6013137aa45044c36cd0",
  "dd6cfb980c8a3659acffa4f0028d4566": {
    "NZ_LS483488.1": [
      "-(232981,233003)"
    ]
  },
  "dd6cfb980c8a3659acffa4f002a38525": {
    "NZ_CP038860.1": [
      "-(2720441,2720463)"
    ]
  },
  "dd6cfb980c8a3659acffa4f002aeea6f": {
    "NC_007295.1": [
      "+(857848,857870)"
    ]
  },
  "dd6cfb980c8a3659acffa4f002ea7404": {
    "NZ_LR214986.1": [
      "+(132513,132535)"
    ]
  }
}
```
Uuid keys corresponds to genomes uuid, associated with sgRNA position in each fasta sequence of the genome. 

### Blast database

Blast database is a classical blast database that contains all genomes. It's created by blast command line tool with `makeblastdb` and it's stored locally. 

### Index database

Index database is a local directory with sgRNA index for each organism. Indexes are int representation of sgRNA based on 2-bits encoding. There is one file per genome, called `<genome_uuid>.index` and each file contains some metadata informations and the list of int indexes with number of occurences. 

```
# 690382 23 twobits
559513471 1
660193231 1
1119347359 1
1137450847 1
...
```
The header line shows the total number of words encoded, the nucleotide-length of the words and code used. The following lines are the int representation with the number of occurences. 

</p>

<p id="add-genome">

## Add new genomes

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
Usage:
    add_genome.py --config <conf> --genomes <genome_list> --location <fasta_folder> [--map <volume_mapper>] [--index <index_file_dump_loc>] [ --min <start_index> --max <stop_index> --cache <pickle_cache> ] [ --debug ] [ --size <batch_size> ] [ --tree ] [ --blast ] [ --force ]

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
    --force  Force add to motif, blast and index databases
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

</p>

<p id="check-consistency">

## Check database consistency

You can check database consistency by pair of collection

If you want to check consistency in motif database, you need to get summary of motif database content by using [ms-db-manager
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

You can check consistency with : 
```
python scripts/check_consistency.py [-h] -c <json file> --db1 <database> --db2 <database> [-m <json file>] [--metadata_out <tsv file>] 

    -h, --help  show this help message and exit
    -m <json file>, --motif_ranks <json file>  json file from ms-db-manager that stores organisms ids and number of occurences. Required if db1 or db2 is motif
    -c <json file>, --config <json file>   json database config file
    --metadata_out <tsv file>   Write metadata for ids found in genome and not in motif if exists. Required if db1 or db2 is genome
    --db1 <database>   First database to check : genome | motif | blast | index
    --db2 <database>   Second database to check : genome | motif | blast | index
```

**Examples** 
* **Check consistency between motif and genome collection**

```
python scripts/check_consistency.py -c config.json --db1 motif --db2 genome -m crispr_rc021_species.json --metadata_out in_genomes_metadata.tsv
```

The script write results in standard output and will display ids present in motif collection and not in genome collection and opposite. It will also write metadata for genomes present in genome collection and not in motifs in --metadata_out file you provide. The format will be the same as input format for add_genome.py.

Example of stdout result : 
```
#Present in motif collection and not in genome collection

#Present in genome collection and not in motif collection
e7bdf2793c3942870a9f84806a6846be
e7bdf2793c3942870a9f84806a68359d
e7bdf2793c3942870a9f84806a681b60
e7bdf2793c3942870a9f84806a684b19
e7bdf2793c3942870a9f84806a6800cc
```

* **Check consistency between genome and blast collection**
```
python scripts/check_consistency.py -c config.json --db1 blast --db2 genome --metadata_out in_genomes_metadata.tsv
```

If you have unconsistency in the database, it's your job to fix problem until consistency.

**Example of cases** 
* I have ids present in genome collection and not in motifs collection : 
  * I want to add this genomes in motif collection : Re-use the `add_genome.py` script for this list of genomes, if fasta and taxon are the same, the same id will be reused in motif collection. If fasta is different, it will be considered as new and current version of the taxon. You can delete the old version if you want. 
  * I want to delete this genomes from genome collection : Use `remove_genome.py`, see Remove genome section in this readme. 

* I have ids present in motifs collection and not in genome collection : 
    * I want to delete this genomes from motif collection : use [ms-db-manager](https://github.com/glaunay/ms-db-manager)

    * I want to add this genomes in genome collection : for now it's not possible to add the genomes and conserve the same ids. We advice to delete from motif collection and re-add with `add_genome.py`, a new id will be used.

* I have ids present in genome collection and not in blast collection : you can use `remove_genome.py` to remove genomes from genome collection or `add_genome.py` with `--blast` option to add genomes in blast collection. 
* I have ids present in blast collection and not in genome collection : it's not possible to retrieve enough information from blast collection to add genomes in genomes collection. We advice to delete all blast database and re-all all genomes with `add_genome.py``

* I have ids present in genome collection and not in index collection : you can use `add_genome.py` with `--index` option to add genomes in index collection. For now, `remove_genome.py` don't remove from index collection but you can delete indexes manually from logged ids. 

* I have ids present in index collection and not in genome collection : it's not possible to retrieve enough information. Delete the files in index and re-add genomes with `add_genome.py`

</p>

<p id="remove-genome">

## Remove genomes

You can **delete a genome from genome and taxon collections** the same way as adding a genome. The script will also **delete from blast database**.

```
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
```

Then **delete the motifs** corresponding to genome with [ms-db-manager
](https://github.com/glaunay/ms-db-manager) (part Delete all sgRNAs relative to a particular specie). For now it's implemented to delete one per one :  
```
node index.js --target 'crispr_rc02_v[0-255]' --remove '<genome id>' --config config.json
```

Don't forget to **delete index files** in file system. 

<p id="replicate-database">

## Replicate database

To have a save of your database, you can replicate each volume. The associated script will create new volume based on current volume name and the saving date. It's a general script that works for every couchDB database. 

```
usage: replicate_database.py [-h] [--db <str>] [--all] --url <str> [--bulk <int>]

Replicate couchDB database

optional arguments:
  -h, --help    show this help message and exit
  --db <str>    Replicate database(s) corresponding to this regular expression
  --all         Replicate all databases in couchDB
  --url <str>   couchDB endpoint
  --bulk <int>  Number of replication to launch simultanously (default: 2)
```

**Example**
```
replicate_database.py --db "crispr_rc03_v[0-9]" --url "http://<username>:<password>@arwen-cdb.ibcp.fr:5984"  
```
Will replicate crispr_rc03_v0 to crispr_rc03_v9 of arwen-cdb

You can check if all volumes are correctly replicated with `check_replication.py`
```
check_replication.py --prefix "crispr_rc03_v" --url "http://<username>:<password>@arwen-cdb.ibcp.fr:5984"
```
Will look if we have at least one backup with the same number of entries for volumes starting with crispr_rc03_v.  
Write summary in stdout and write correct and incorrect backups on a log file.

</p>

<p id="update-database">

## Update database

To have the latest refseq database version, you have to follow some steps : 

### 1. Download new genomes 

To do that, you need [ncbi-genome-download](https://github.com/cecilpert/ncbi-genome-download) tool (modified version based on [kblin tool
](https://github.com/kblin/ncbi-genome-download)). 
There's a module on arwen : 
```
module load ncbi-genome-download
```

To download new genomes : 
```
download_refseq.sh <output metadata> <output directory>
```

It will uses ncbi-genome-download to download representative and reference complete bacteria genomes in fasta format. If you provide an output directory with fasta already present, it will not re-download them. 
It will write metadata file that you can directly provide to <add_genome.py> script. 

**Example** 
```
download_refseq.sh metadata_reference_representative_27-07-20.tsv ../fasta_folder/
```

### 2. Update informations

You can launch `update_database.py` to have informations about updated database stored in files. 

```
Usage:
    update_database.py --current <metadata_file> --new <metadata_file>

Options:
    -h --help
    --current <metadata_file> assembly_summary file for current version of database
    --new <metadata_file> assembly_summary file for new version of database
```

**Example**
```
update_database.py --current ../../crispr_rc03/list_genomes.tsv --new metadata_reference_representative_27-07-20.tsv
```

On stdout, it will write summary of update : 
```
== 2886 common genomes
== 1 updated genomes
GCF_001442815.1 => GCF_001442815.2
= updated genomes list in updated_genomes.tsv
== 113 new genomes
GCF_010731795.1
GCF_000011705.1
GCF_003952225.1
....
= new genomes list in new_genomes.tsv
== 27 deprecated genomes
GCF_000011485.1
GCF_006542705.1
GCF_002355995.1
...
= deprecated genomes list in deprecated_genomes.tsv
``` 

It created separated metadata files for each category that you can directly use with `add_genome.py` or `remove_genome.py` : `updated_genomes.tsv`, `new_genomes.tsv` and `deprecated_genomes.tsv`

### 3. Add or remove genomes

```
add_genome.py --config ../config.json --genomes updated_genomes.tsv --location ../fasta_folder --map ../4letters_prefix_rule.json
--index ../index --tree --blast
```

```
add_genome.py --config ../config.json --genomes new_genomes.tsv --location ../fasta_folder --map ../4letters_prefix_rule.json
--index ../index --tree --blast
```

If you want to remove deprecated genomes :
```
remove_genome.py --config ../config.json --genomes deprecated_genomes.tsv --location ../fasta_folder --tree --blast
```
You also need to delete them from index directory and motifs database (see [Remove genomes](#remove-genome) section.))

</p>










