import json, copy, pickle

from typing import TypedDict, Tuple, Dict
from typeguard import typechecked

import pycouch.wrapper as wrapper

from CSTB_database_manager.engine.word_detect import sgRNAfastaSearch
from CSTB_database_manager.engine.wordIntegerIndexing import indexAndOccurence as computeMotifsIndex
import CSTB_database_manager.db.taxon as taxonDBHandler
import CSTB_database_manager.db.genome as genomeDBHandler
import CSTB_database_manager.utils.error as error
from CSTB_database_manager.utils.io import fileHash as fastaHash
from CSTB_database_manager.utils.io import Zfile as zFile
from  CSTB_database_manager.db.genome import GenomeEntity as tGenomeEntity

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
    
    def setDebugMode(self, value=True):
        wrapper.DEBUG_MODE = value

    def setMotifAgent(self, mappingRuleFile):
        with open(mappingRuleFile, 'rb') as fp:
            self.wrapper.setKeyMappingRules(json.load(fp))
        print(f"Loaded {len(self.wrapper.queue_mapper)} volumes mapping rules" )
    
    def getGenomeEntity(self, fastaMd5):#:str):
        return self.genomedb.get(fastaMd5)

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
        with zFile(fasta) as f: 
            for l in f: 
                if l.startswith('>'):
                    ref = l.split(" ")[0].lstrip(">")
                    dic_size[ref] = 0
                else:
                    dic_size[ref] += len(l.rstrip())           
        return dic_size

        

    def updateGenome(self):
        print("UPDATE")


    def addFastaMotifs(self, fastaFileList, batchSize=10000, indexLocation=None, cacheLocation=None):
        for fastaFile in fastaFileList:
            sgRNA_data, uuid, ans = self.addFastaMotif(fastaFile, batchSize)
            if cacheLocation and not ans is None:
                fPickle = cacheLocation + "/" + uuid + ".p"
                pickle.dump(sgRNA_data, open(fPickle, "wb"), protocol=3)
                print(f"databaseManager::addFastaMotif:pickling of \"{len(sgRNA_data.keys())}\" sgnRNA motifs wrote to {fPickle}")
       
            if indexLocation:
                indexLen = self.addIndexMotif(indexLocation, sgRNA_data, uuid)
                print(f"databaseManager::addFastaMotif:indexation of \"{indexLen}\" sgnRNA motifs wrote to {indexLocation}/{uuid}.index")
       
    def addIndexMotif(self, location, sgnRNAdata, uuid):
        indexData = computeMotifsIndex(sgnRNAdata)
        with open (location + '/' + uuid + '.index', 'w') as fp:
            fp.write(str(len(indexData)) + "\n")
            for datum in indexData:
                fp.write( ' '.join([str(d) for d in datum]) + "\n")
        return len(indexData)

    def addFastaMotif(self, fastaFile, batchSize):   
        uuid = fastaHash(fastaFile)
        genomeEntity = self.getGenomeEntity(uuid)
        if not genomeEntity:
            raise error.NoGenomeEntity(fastaFile)
        sgRNA_data = sgRNAfastaSearch(fastaFile, uuid)
        allKeys = list(sgRNA_data.keys())
        if not self.wrapper.hasKeyMappingRules:
            print(f"databaseManager::addFastaMotif:Without mapping rules, {len(allKeys)} computed sgRNA motifs will not be inserted into database")
            return (sgRNA_data, uuid, None)
        
        print(f"databaseManager::addFastaMotif:Slicing \"{uuid}\" to volDocAdd its {len(allKeys)} genomic sgRNA motif")       
        for i in range(0,len(allKeys), batchSize):
          
            j = i + batchSize if i + batchSize < len(allKeys) else len(allKeys)
            legit_keys = allKeys[i:j]
            d = { k : sgRNA_data[k] for k in legit_keys }
            print(f"databaseManager::addFastaMotif:Attempting to volDocAdd {len(d.keys())} sets sgRNA keys")
       
            r = self.wrapper.volDocAdd(d)
        return (sgRNA_data, uuid, r)
