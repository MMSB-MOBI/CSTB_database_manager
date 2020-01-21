import CSTB_database_manager.taxonDB as taxonDBHandler
import CSTB_database_manager.genomeDB as genomeDBHandler
import pycouch.wrapper_class as wrapper
import json
from typing import TypedDict, Tuple, Dict
from typeguard import typechecked
import hashlib
import copy
import CSTB_database_manager.error as error

class ConfigType(TypedDict):
    url: str
    user: str
    password: str
    taxondb_name: str
    genomedb_name: str

@typechecked
class DatabaseManager():
    """
    Database manager object

    :param wrapper: pouet
    :type wrapper: pyCouch wrapper
    """
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
    
    def addGenomeOld(self, fasta: str, name: str, taxid: int = None, gcf: str = None, acc: str = None):
        print(f"INFO : Add genome\nfasta : {fasta}\nname : {name}\ntaxid : {taxid}\ngcf assembly : {gcf}\naccession number : {acc}")
        # Check if fasta already exists in genomeDB
        hasher = hashlib.md5()
        with open(fasta, "rb") as f:
            buf = f.read()
            hasher.update(buf)
        fasta_md5 = hasher.hexdigest()

        genomeDB_doc = self.genomedb.get(fasta_md5)

        if not genomeDB_doc : #create doc, not complete now because we don't have the taxon uuid
            print("Genome not found, will be inserted")
            genomeDB_doc = self.genomedb.create_insert_doc(fasta_md5, gcf, acc)
        else: # Fasta exists, check if gcf and acc are the same ? 
            if genomeDB_doc["gcf_assembly"] != gcf or genomeDB_doc["accession_number"] != acc:
                print(f'WARN: Fasta already exists with other gcf and/or accession (gcf : {genomeDB_doc["gcf_assembly"]}, accession : {genomeDB_doc["accession_number"]}). Update genome if you want to give new attributes.')
                return

        # Check if taxon already exists in taxonDB
        taxonDB_doc = self.taxondb.get(name, taxid)

        if not taxonDB_doc : 
            print("Taxon not found, will be inserted")
            taxonDB_doc = self.taxondb.create_insert_doc(name, taxid)
        
        # Get final docs after match making
        final_genomeDB_doc, final_taxonDB_doc = self._makeMatching(genomeDB_doc, taxonDB_doc)
        #Insert final docs if change

        #Replace this by a store function associated with Doc class
        if genomeDB_doc != final_genomeDB_doc:
            print("Insert genome")
            self.genomedb.add(final_genomeDB_doc)
        
        if taxonDB_doc != final_taxonDB_doc:
            print("Insert taxon")
            self.taxondb.add(final_taxonDB_doc)

    def addGenome(self, fasta: str, name: str, taxid: int = None, gcf: str = None, acc: str = None):
        print(f"INFO : Add genome\nfasta : {fasta}\nname : {name}\ntaxid : {taxid}\ngcf: {gcf}\nacc: {acc}")

        hasher = hashlib.md5()
        try:
            with open(fasta, "rb") as f:
                buf = f.read()
                hasher.update(buf)
            fasta_md5 = hasher.hexdigest()
        except FileNotFoundError:
            print(f"Can't add your entry because fasta file is not found.")
            return

        try:
            genome_entity = self.genomedb.get(fasta_md5, gcf, acc)
        except error.DuplicateError as e: 
            print(f"Can't add your entry because DuplicateError in genome database \nReason : \n{e}")
            return
        except error.ConsistencyError as e: 
            print(f"Can't add your entry because ConsistencyError in genome database \nReason : \n{e}")
            return
        
        if not genome_entity:
            size = self._get_fasta_size(fasta)
            genome_entity = self.genomedb.createNewGenome(fasta_md5, size, gcf, acc)

        try:
            taxon_entity = self.taxondb.get(name, taxid)
        except error.DuplicateError as e:
            print(f"Can't add your entry because DuplicateError in taxon database \nReason : \n{e}")
            return
        except error.ConsistencyError as e:
            print(f"Can't add your entry because ConsistencyError in taxon database \nReason : \n{e}")
            return
        
        if not taxon_entity:
            taxon_entity = self.taxondb.createNewTaxon(name, taxid)
        
        try:
            self.bind(genome_entity, taxon_entity)

        except error.LinkError as e:
            print(f"Can't add your entry because LinkError\nReason : \n{e}")
            return
        
        except error.VersionError as e:
            print(f"Can't add your entry because VersionError\nReason : \n{e}")
            return

        genome_entity.store()
        taxon_entity.store()
        
    def bind(self, genome, taxon):
        if genome.isInDB():
            if taxon.isInDB():
                if genome.taxon == taxon._id:
                    if taxon.current == genome._id:
                        print("Genome already exists as current version")
                    else:
                        if genome._id in taxon.genomeColl:
                            raise error.VersionError(f'This genome exists as an older version of Taxon (name : {taxon.name}, taxid : {taxon.taxid})')
                        else:
                            raise error.LinkError("Taxon link in Genome but no Genome link in Taxon.")
                else:
                    current_taxon = self.taxondb.getFromID(genome.taxon)#Not really useful interrogation, just for print information
                    raise error.LinkError(f'Genome already exists but associated with an other Taxon (name : {current_taxon.name}, taxid : {current_taxon.taxid}). Update this taxon or delete genome if you really want to add it.')
                
            else:
                current_taxon = self.taxondb.getFromID(genome.taxon)#Not really useful interrogation, just for print information
                raise error.LinkError(f'Genome already exists but associated with an other Taxon (name : {current_taxon.name}, taxid : {current_taxon.taxid}). Update this taxon or delete genome if you really want to add it.')
        else:
            if taxon.isInDB():
                print(f'New genome version for Taxon (name: {taxon.name}, taxid {taxon.taxid}')
                genome.taxon = taxon._id
                taxon.current = genome._id
                taxon.genomeColl.append(genome._id)

            else:
                print("Genome and taxon are new")
                genome.taxon = taxon._id
                taxon.current = genome._id
                taxon.genomeColl = [genome._id]
    
    def _get_fasta_size(self, fasta: str) -> Dict:
        dic_size = {}
        with open(fasta) as f: 
            for l in f: 
                if l.startswith('>'):
                    ref = l.split(" ")[0].lstrip(">")
                    dic_size[ref] = 0
                else:
                    dic_size[ref] += len(l.rstrip())           
        return dic_size

        

    def updateGenome(self):
        print("UPDATE")


