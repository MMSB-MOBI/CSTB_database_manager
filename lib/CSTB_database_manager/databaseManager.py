import json, copy, pickle

from typing import TypedDict, Tuple, Dict
from typeguard import typechecked

import pycouch.wrapper_class as wrapper

from CSTB_database_manager.engine.word_detect import sgRNAfastaSearch
import CSTB_database_manager.db.taxon as taxonDBHandler
import CSTB_database_manager.db.genome as genomeDBHandler
import CSTB_database_manager.db.tree as treeDBHandler
import CSTB_database_manager.utils.error as error
from CSTB_database_manager.utils.io import fileHash as fastaHash

from  CSTB_database_manager.db.genome import GenomeEntity as tGenomeEntity
import CSTB_database_manager.engine.taxonomic_tree as tTree

import logging
logging.basicConfig(level = logging.INFO, format='%(levelname)s\t%(message)s')

class ConfigType(TypedDict):
    url: str
    user: str
    password: str
    taxondb_name: str
    genomedb_name: str
    treedb_name: str

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
        self.taxondb = self._init_genomedb(config["taxondb_name"], taxonDBHandler.TaxonDB)
        self.genomedb = self._init_genomedb(config["genomedb_name"], genomeDBHandler.GenomeDB)
        self.treedb = self._init(config["treedb_name"], treeDBHandler.TreeDB)

    def _load_config(self, config_file:str)-> ConfigType:
        with open(config_file) as f:
            config: ConfigType = json.load(f)
        return config

    def _init_wrapper(self, url: str, admin: Tuple[str,str]) -> wrapper.Wrapper:
        wrapperObj = wrapper.Wrapper(url, admin)
        if not wrapperObj.couchPing():
            raise Exception(f"Can't ping {url}")
        
        return wrapperObj

    def _init(self, database_name, database_obj):
        if not self.wrapper.couchTargetExist(database_name):
            print(f"INFO: Create {database_name}")
            self.wrapper.couchCreateDB(database_name)
        return database_obj(self.wrapper, database_name)
    
    def setDebugMode(self, value=True):
        wrapper.DEBUG_MODE = value

    def setMotifAgent(self, mappingRuleFile):
        with open(mappingRuleFile, 'rb') as fp:
            self.wrapper.setKeyMappingRules(json.load(fp))
        print(f"Loaded {len(self.wrapper.queue_mapper)} volumes mapping rules" )
    
    def getGenomeEntity(self, fastaMd5:str):
        return self.genomedb.get(fastaMd5)

    def addGenomeOld(self, fasta: str, name: str, taxid: int = None, gcf: str = None, acc: str = None):
        print(f"INFO : Add genome\nfasta : {fasta}\nname : {name}\ntaxid : {taxid}\ngcf assembly : {gcf}\naccession number : {acc}")
        # Check if fasta already exists in genomeDB
        fasta_md5 = fastaHash(fasta)
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

        try :
            fasta_md5 = fastaHash(fasta)
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

    def _get_md5(self, fasta:str) -> str:
        hasher = hashlib.md5()
        with open(fasta, "rb") as f:
            buf = f.read()
            hasher.update(buf)
        fasta_md5 = hasher.hexdigest()
        return fasta_md5

    def removeGenome(self, fasta: str, gcf: str = None, acc:str = None): 
        print("INFO : Remove genome")
        try:
            md5 = self._get_md5(fasta)
        except FileNotFoundError:
            print("Can't remove your entry because fasta file is not found")
            return
        try: 
            genome = self.genomedb.get(md5)
        except error.DuplicateError as e:
            print(f"Can't remove your entry because of DuplicateError\nreason: {e}")
            return
        except error.ConsistencyError as e :
            print(f"Can't remove your entry because of ConsistencyError\nreason: {e}")
            return
        except wrapper.CouchWrapperError as e:
            print(f"Can't remove your entry because of CouchWrapperError\nreason: {e}")
            return
        if not genome:
            print(f"Genome doesn't exist in genome database")
            return

        print(f"Genome : {genome._id}")
        print(f"Corresponding taxon is {genome.taxon}")

        try: 
            taxon = self.taxondb.getFromID(genome.taxon)
        except wrapper.CouchWrapperError as e : 
            print(f"Can't remove your entry because of CouchWrapperError\nreason: {e}")
            return

        if not taxon: 
            raise error.ConsistencyError(f"Associated taxon {genome.taxon} doesn't exist in taxon database")
        
        if not genome._id in taxon.genomeColl: 
            raise error.ConsistencyError(f"Genome {genome._id} is not linked with its taxon {taxon._id}")

        taxon.genomeColl.remove(genome._id)

        if not taxon.genomeColl:
            print(f"Your genome was the only version of the taxon (name : {taxon.name}, taxid : {taxon.taxid}). Taxon will be deleted.")
            genome.remove()
            taxon.remove()

        else: 
            print(f"Delete this version of Taxon (name : {taxon.name}, taxid : {taxon.taxid}). Current version become the previous one.")
            taxon.current = taxon.genomeColl[-1]
            genome.remove()
            taxon.store()


    
    def createTree(self):
        # Find all taxids
        try:
            total_taxons = self.taxondb.number_of_entries()
        except wrapper.CouchWrapperError as e:
            logging.error(f"CouchWrapperError when try to get number of entries\n{e}")
            return

        mango_query = {
            "selector": {
                "taxid" : {"$ne": None}
            },
            "limit":total_taxons
        }
        try:
            docs = self.taxondb.execute_mango_query(mango_query)
        except error.MangoQueryError as e:
            logging.error(f"MangoQueryError\n{e}")
            return

        logging.info(f"{len(docs)} taxids found")
        dic_taxid = {d["taxid"]:d["name"] for d in docs}

        mango_query = {
            "selector": {
                "taxid" : {"$eq": None}
            },
            "limit": total_taxons
        }
        try:
            docs = self.taxondb.execute_mango_query(mango_query)
        except error.MangoQueryError as e:
            logging.error(f"MangoQueryError\n{e}")
            return

        logging.info(f"{len(docs)} other taxon found")
        other_taxons = [ d["name"] for d in docs ]

        if not dic_taxid and not other_taxons:
            print(f"No taxid found in {self.taxondb.db_name}")
            return

        # Create tree

        tree = tTree.create_tree(dic_taxid, other_taxons)
        tree_json = tree.get_json()
        
        tree_entity = self.treedb.createNewTree(tree_json)
        tree_entity.store()
        
        
        
    def addFastaMotifs(self, fastaFileList, batchSize=10000, cacheLocation=None):
        for fastaFile in fastaFileList:
            sgRNA_data, uuid, ans = self.addFastaMotif(fastaFile, batchSize)
            if cacheLocation:
                pickle.dump(sgRNA_data, open(cacheLocation + "/" + uuid + ".p", "wb"), protocol=3)
       
    def addFastaMotif(self, fastaFile, batchSize):   
        uuid = fastaHash(fastaFile)
        genomeEntity = self.getGenomeEntity(uuid)
        if not genomeEntity:
            raise error.NoGenomeEntity(fastaFile)
        sgRNA_data = sgRNAfastaSearch(fastaFile, uuid)
        allKeys = list(sgRNA_data.keys())
        print(f"databaseManager::addFastaMotif:Slicing \"{uuid}\" to volDocAdd its {len(allKeys)} genomic sgRNA motif")
       
        for i in range(0,len(allKeys), batchSize):
          
            j = i + batchSize if i + batchSize < len(allKeys) else len(allKeys)
            legit_keys = allKeys[i:j]
            d = { k : sgRNA_data[k] for k in legit_keys }
            print(f"databaseManager::addFastaMotif:Attempting to volDocAdd {len(d.keys())} sets sgRNA keys")
       
            r = self.wrapper.volDocAdd(d)
        return (sgRNA_data, uuid, r)