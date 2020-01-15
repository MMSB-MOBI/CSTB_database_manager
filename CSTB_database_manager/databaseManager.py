import CSTB_database_manager.taxonDB as taxonDBHandler
import CSTB_database_manager.genomeDB as genomeDBHandler
import pycouch.wrapper_class as wrapper
import json
from typing import TypedDict, Tuple, Dict
from typeguard import typechecked
import hashlib

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

    def makeMatching(self, genomeDB_doc: {}, taxonDB_doc:{}) -> Tuple[genomeDBHandler.GenomeDoc, taxonDBHandler.TaxonDoc]:

        if not genomeDB_doc.get("taxon") and not taxonDB_doc.get("current"): #All is new. Make correspondance between the 2.
            genomeDB_doc["taxon"] = taxonDB_doc["_id"]
            taxonDB_doc["current"] = genomeDB_doc["_id"]
            taxonDB_doc["genomeColl"] = [genomeDB_doc["_id"]]
            return genomeDB_doc, taxonDB_doc

        if genomeDB_doc.get("taxon") and taxonDB_doc.get("current"):
            if genomeDB_doc["taxon"] == taxonDB_doc["_id"] and taxonDB_doc["current"] == genomeDB_doc["_id"]:
                print("Genome already exists and it's already current version")
                return genomeDB_doc, taxonDB_doc

        

        

