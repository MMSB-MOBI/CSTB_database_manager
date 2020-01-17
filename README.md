# CSTB_database_manager

CSTB database is a collection of 3 types of couchDB databases (taxon_db, genome_db, tree_db and crispr_motif) and some local index files in the system.

## Initialize database
To initialize CSTB database via databaseManager, you must provide a json config file formatted like : 
```
{
    "url": "http://localhost:5984",
    "user": "couch_agent",
    "password": "couch",
    "taxondb_name": "taxon_db",
    "genomedb_name": "genome_db"
}
```

To initialize database manager object, simply do : 
```
import CSTB_database_manager.databaseManager as dbManager
db = dbManager.databaseManager(config_file_path)
```

## Taxon and Genome databases
Taxon and genome databases are linked, modification in one triggers modification in the other.

### Structure 
#### Taxon database
Taxon database is a collection of this type of documents : 
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
`genomeColl` and `current` are references to Genome document.

#### Genome database
Genome database is a collection of this type of documents : 
```
{
  "_id": "32cd400c3f5997cfdd3abd290e0bb43f",
  "_rev": "1-e6b9861dc339a082d9ca6a1a06fe6c8d",
  "fasta_md5": "6399150c8cbed82ed0f50b8d6b09f8bc",
  "taxon": "32cd400c3f5997cfdd3abd290e0bbebc"
}
```
`taxon` are reference to Taxon document. 

### Operations

#### Add a new element 

```
db.addGenome(fasta: str, name: str, taxid: int = None)
```
`fasta` : path to fasta file  
`name` : taxon name  
`taxid` : (optional) taxon taxid

Insertion into database will be done when : 
  * fasta doesn't exist in genome database and taxon doesn't exist in taxon database
  * fasta doesn't exist in genome database and taxon exists in taxon database -> fasta is considered as a new version for taxon. 
  
Insertion will not be done and we ask for update when: 
  * fasta exists in genome database but is linked with an other taxon -> you need to update taxon or delete genome to insert
  * fasta exists in genome database but is an old version for taxon -> you need to change taxon version  
  
#### Update genome
 
#### Update taxon

#### Remove genome

#### Remove taxon

#### Change taxon version
