import CSTB_database_manager.taxonDB as taxonDBHandler
import CSTB_database_manager.genomeDB as genomeDBHandler
import pycouch.wrapper_class as wrapper
import json
from typing import TypedDict, Tuple, Dict
from typeguard import typechecked
import hashlib
import copy

class ConfigType(TypedDict):
    url: str
    user: str
    password: str
    taxondb_name: str
    genomedb_name: str

@typechecked
class databaseManager():
    def __init__(self, config_file:str) -> None:
        if not isinstance(config_file, str):
            raise TypeError(f"config_file must be str not {type(config_file)}")

        config: ConfigType = self._load_config(config_file)
        self.wrapper: wrapper.Wrapper = self._init_wrapper(config["url"], (config["user"], config["password"]))
        self.taxondb: taxonDBHandler.TaxonDB = self._init_taxondb(config["taxondb_name"])
        self.genomedb : genomeDBHandler.GenomeDB = self._init_genomedb(config["genomedb_name"])

    def _load_config(self, config_file:str)-> ConfigType:
        with open(config_file) as f:
            config: ConfigType = json.load(f)
        return config

    def _init_wrapper(self, url: str, admin: Tuple[str,str]) -> wrapper.Wrapper:
        wrapperObj = wrapper.Wrapper(url, admin)
        if not wrapperObj.couchPing():
            raise Exception(f"Can't ping {url}")
        
        return wrapperObj

    def _init_taxondb(self, taxondb_name: str) -> taxonDBHandler.TaxonDB:
        # Check taxondb existence, if not create it
        if not self.wrapper.couchTargetExist(taxondb_name): # Maybe do this in TaxonDB class?
            print(f"INFO: Create {taxondb_name}")
            self.wrapper.couchCreateDB(taxondb_name)
        return taxonDBHandler.TaxonDB(self.wrapper, taxondb_name)
    
    def _init_genomedb(self, genomedb_name: str) -> genomeDBHandler.GenomeDB:
        if not self.wrapper.couchTargetExist(genomedb_name): #Maybe do this in GenomeDB class?
            print(f"INFO: Create {genomedb_name}")
            self.wrapper.couchCreateDB(genomedb_name)
        return genomeDBHandler.GenomeDB(self.wrapper, genomedb_name)
    
    def addGenome(self, fasta: str, name: str, taxid: int = None):
        print(f"INFO : Add genome\nfasta : {fasta}\nname : {name}\ntaxid : {taxid}")
        # Check if fasta already exists in genomeDB
        hasher = hashlib.md5()
        with open(fasta, "rb") as f:
            buf = f.read()
            hasher.update(buf)
        fasta_md5 = hasher.hexdigest()
        genomeDB_doc = self.genomedb.get(fasta_md5)

        if not genomeDB_doc : #create doc, not complete now because we don't have the taxon uuid
            print("Genome not found, will be inserted")
            genomeDB_doc = self.genomedb.create_insert_doc(fasta_md5)
        
        # Check if taxon already exists in taxonDB
        taxonDB_doc = self.taxondb.get(name, taxid)

        if not taxonDB_doc : 
            print("Taxon not found, will be inserted")
            taxonDB_doc = self.taxondb.create_insert_doc(name, taxid)

        # Get final docs after match making
        final_genomeDB_doc, final_taxonDB_doc = self.makeMatching(genomeDB_doc, taxonDB_doc)
        #Insert final docs if change

        if genomeDB_doc != final_genomeDB_doc:
            print("Insert genome")
            self.genomedb.add(final_genomeDB_doc)
        
        if taxonDB_doc != final_taxonDB_doc:
            print("Insert taxon")
            self.taxondb.add(final_taxonDB_doc)

    def makeMatching(self, genomeDB_doc: genomeDBHandler.GenomeDoc, taxonDB_doc:taxonDBHandler.TaxonDoc) -> Tuple[genomeDBHandler.GenomeDoc, taxonDBHandler.TaxonDoc]:
        new_genome_doc = copy.deepcopy(genomeDB_doc)
        new_taxon_doc = copy.deepcopy(taxonDB_doc)

        if not genomeDB_doc.get("taxon") and not taxonDB_doc.get("current"): #All is new. Make correspondance between the 2.
            new_genome_doc["taxon"] = taxonDB_doc["_id"]
            new_taxon_doc["current"] = genomeDB_doc["_id"]
            new_taxon_doc["genomeColl"] = [genomeDB_doc["_id"]]
    
        elif genomeDB_doc.get("taxon") and taxonDB_doc.get("current"):
            if genomeDB_doc["taxon"] == taxonDB_doc["_id"]:
                if taxonDB_doc["current"] == genomeDB_doc["_id"]:
                    print("Genome already exists as current version")
                elif genomeDB_doc["_id"] in taxonDB_doc["genomeColl"]:
                    
                    print(f'This genome exists as an older version of Taxon (name : {taxonDB_doc["name"]}, taxid : {taxonDB_doc["taxid"]})')
                    # Do we really want to replace current version by old ? Maybe better to create function dedicated to this in taxonDB ? 
                else:
                    raise Exception("Taxon link in Genome but no Genome link in Taxon.")
            else:
                current_taxon = self.wrapper.couchGetDoc(self.taxondb.db_name, genomeDB_doc["taxon"])#Not really useful interrogation, just for print information
                print(f'Genome already exists but associated with an other Taxon (name : {current_taxon["name"]}, taxid : {current_taxon["taxid"]}). Update this taxon or delete genome if you really want to add it.')
        
        elif not genomeDB_doc.get("taxon") and taxonDB_doc.get("current"):
            print(f'New genome version for taxon {taxonDB_doc["name"]}')
            new_genome_doc["taxon"] = taxonDB_doc["_id"]
            new_taxon_doc["current"] = genomeDB_doc["_id"]
            new_taxon_doc["genomeColl"].append(genomeDB_doc["_id"])
        
        elif genomeDB_doc.get("taxon") and not taxonDB_doc.get("current"):
            #Genome exists with another taxon
            current_taxon = self.wrapper.couchGetDoc(self.taxondb.db_name, genomeDB_doc["taxon"])
            print(f'Genome already exists but associated with an other Taxon (name : {current_taxon["name"]}, taxid : {current_taxon["taxid"]}). Update this taxon or delete genome if you really want to add it.')
            
        else:
            #Really weird if it goes here
            print("WARN: CASE NOT HANDLED")
            exit()

        return new_genome_doc, new_taxon_doc

        

        

