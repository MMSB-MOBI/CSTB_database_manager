import json, copy, pickle

from typing import TypedDict, Tuple, Dict, Set, Optional, List
from typeguard import typechecked

import pycouch.wrapper as wrapper

from CSTB_core.engine.word_detect import sgRNAfastaSearch
from CSTB_core.utils.io import sgRNAIndexWriter
from CSTB_core.engine.wordIntegerIndexing import indexAndMayOccurence as computeMotifsIndex
from CSTB_core.engine.wordIntegerIndexing import getEncoding
import CSTB_database_manager.db.couch.taxon as taxonDBHandler
import CSTB_database_manager.db.couch.genome as genomeDBHandler
import CSTB_database_manager.db.blast as blastDBHandler

import CSTB_database_manager.utils.error as error
from CSTB_core.utils.io import fileHash as fastaHash
from CSTB_core.utils.io import Zfile as zFile
from  CSTB_database_manager.db.couch.genome import GenomeEntity as tGenomeEntity
# GL for sbatch, temporary hack
import CSTB_database_manager.db.couch.tree as treeDBHandler
import CSTB_database_manager.engine.taxonomic_tree as tTree
from CSTB_core.utils.io import zFastaReader
import logging

class ConfigType(TypedDict):
    url: str
    user: str
    password: str
    taxondb_name: str
    genomedb_name: str
    treedb_name: str
    blastdb_path : str

@typechecked
class DatabaseManager():
    """
    Database manager object
    """
    def __init__(self, config_file:str) -> None:
        if not isinstance(config_file, str):
            raise TypeError(f"config_file must be str not {type(config_file)}")
        try:
            config: ConfigType = self._load_config(config_file)
        except Exception as e:
            logging.error(f"Error while load config\n{e}")
            exit()
        self.wrapper: wrapper.Wrapper = self._init_wrapper(config["url"], (config["user"], config["password"]))
        self.taxondb = self._init(config["taxondb_name"], taxonDBHandler.TaxonDB)
        self.genomedb = self._init(config["genomedb_name"], genomeDBHandler.GenomeDB)
        # GL for sbatch, temporary hack
        self.treedb = self._init(config["treedb_name"], treeDBHandler.TreeDB)

        if not "blastdb_path" in config:
            logging.info(f"DatabaseManager:init:Warning no blast database specified")
            self.blastdb = None
        else:
            self.blastdb = blastDBHandler.connect(config["blastdb_path"])

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
            logging.info(f"Create {database_name}")
            self.wrapper.couchCreateDB(database_name)
        return database_obj(self.wrapper, database_name)

    def setDebugMode(self, value=True):
        wrapper.DEBUG_MODE = value

    def setMotifAgent(self, mappingRuleFile):
        with open(mappingRuleFile, 'rb') as fp:
            self.wrapper.setKeyMappingRules(json.load(fp))
        logging.info(f"Loaded {len(self.wrapper.queue_mapper)} volumes mapping rules" )
    
    def getGenomeEntity(self, fastaMd5):#:str):
        return self.genomedb.get(fastaMd5)

    def addBlast(self, fastaList):
        for zFasta in fastaList:
            fasta_md5 = fastaHash(zFasta)
            genomElem = self.genomedb.get(fasta_md5)
            for header, seq, _id  in zFastaReader(zFasta):
                _header = f">{genomElem._id}|{header.replace(r'/^>//', '')}"
                self.blastdb.add(_header, seq)
        self.blastdb.close()
        
    def addGenome(self, fasta: str, name: str, taxid: int = None, gcf: str = None, acc: str = None):
        """
        Take informations about genome and corresponding taxon and insert them in databases.

        :param fasta: Path to fasta file
        :param name: Taxon name
        :param taxid: Taxid if available
        :param gcf: GCF accession for assembly if available
        :param acc: accession number if available

        :type fasta: path
        :type name: str
        :type taxid: int
        :type gcf: str
        :type acc: str
        """
        logging.info(f"Add genome\nfasta : {fasta}\nname : {name}\ntaxid : {taxid}\ngcf: {gcf}\nacc: {acc}")
        fasta_name = fasta.split("/")[-1]
        try :
            fasta_md5 = fastaHash(fasta)
        except FileNotFoundError:
            logging.error(f"Can't add your entry because fasta file is not found.")
            return    

        try:
            genome_entity = self.genomedb.get(fasta_md5, gcf, acc)
        except error.DuplicateError as e: 
            logging.error(f"Can't add your entry because DuplicateError in genome database \nReason : \n{e}")
            return
        except error.ConsistencyError as e: 
            logging.error(f"Can't add your entry because ConsistencyError in genome database \nReason : \n{e}")
            return
        
        #Check if genome_entity contains headers and fasta_name or if it's old version, and update it if necessary. (Temporary hack until all is updated)
        if genome_entity and (not genome_entity.headers or not genome_entity.fasta_name):
            #WARN: Duplicated code
            try:
                size, headers = self._proceed_fasta(fasta)
            except error.FastaHeaderConflict as e:
                logging.error(f"Can't add your entry because FastaHeaderConflict\n{e}")
                return

            logging.warn(f"Your genome entry already exists but as old version (no headers and no fasta_name), the entry will be updated")
            try: 
                genome_entity.update(headers = headers, fasta_name = fasta_name)
            except error.NotAvailableKeys as e: 
                logging.error(f"Can't update your entry because NotAvailableKeys\n{e}")
                return

        if not genome_entity:
            try:
                size, headers = self._proceed_fasta(fasta)
            except error.FastaHeaderConflict as e:
                logging.error(f"Can't add your entry because FastaHeaderConflict\n{e}")
                return
                
            genome_entity = self.genomedb.createNewGenome(fasta_md5, size, headers, fasta_name, gcf, acc)

        try:
            taxon_entity = self.taxondb.get(name, taxid)
        except error.DuplicateError as e:
            logging.error(f"Can't add your entry because DuplicateError in taxon database \nReason : \n{e}")
            return
        except error.ConsistencyError as e:
            logging.error(f"Can't add your entry because ConsistencyError in taxon database \nReason : \n{e}")
            return
        
        if not taxon_entity:
            taxon_entity = self.taxondb.createNewTaxon(name, taxid)
        
        try:
            self.bind(genome_entity, taxon_entity)

        except error.LinkError as e:
            logging.error(f"Can't add your entry because LinkError\nReason : \n{e}")
            return
        
        except error.VersionError as e:
            logging.error(f"Can't add your entry because VersionError\nReason : \n{e}")
            return

        genome_return_status = genome_entity.store()
        taxon_return_status = taxon_entity.store()

        return genome_return_status
        
    def bind(self, genome, taxon):
        """Make the link between genome and taxon. Add genome uuid in taxon entry and taxon uuid in genome entry with consistency checks.
        
        :param genome: GenomeEntity that represents a genome couch entry
        :type genome: CSTB_database_manager.genome.GenomeEntity
        :param taxon: TaxonEntity that represents a taxon couch entry
        :type taxon: CSTB_database_manager.genome.TaxonEntity
        :raises error.VersionError: Raise if genome already exists as an older version of taxon. 
        :raises error.LinkError: Raise if links already exists and don't correspond.
        """
        if genome.isInDB():
            if taxon.isInDB():
                if genome.taxon == taxon._id:
                    if taxon.current == genome._id:
                        logging.warn("Genome already exists as current version")
                    else:
                        if genome._id in taxon.genomeColl:
                            raise error.VersionError(f'This genome ({genome._id}) exists as an older version of Taxon ({taxon._id} name : {taxon.name}, taxid : {taxon.taxid})')
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
                logging.info(f'New genome version for Taxon (name: {taxon.name}, taxid {taxon.taxid}')
                genome.taxon = taxon._id
                taxon.current = genome._id
                taxon.genomeColl.append(genome._id)

            else:
                logging.info("Genome and taxon are new")
                genome.taxon = taxon._id
                taxon.current = genome._id
                taxon.genomeColl = [genome._id]

    def _proceed_fasta(self, fasta:str):
        """Proceed fasta file to get sequences sizes and store sequences headers
        
        :param fasta: Path to fasta file
        :type fasta: str
        :return: 2 dictionnaries, first with size for each fasta subsequences and second with complete headers for each fasta subsequences
        """

        dic_size = {}
        dic_headers = {}

        with zFile(fasta) as f: 
            for l in f: 
                if l.startswith('>'):
                    ref = l.split(" ")[0].lstrip(">")
                    complete_header = l.rstrip("\n").lstrip(">")
                    if ref in dic_size:
                        raise error.FastaHeaderConflict(f"Two fasta header have same first identifiant : {ref}. Change fasta headers to insert this genome.")
                    dic_size[ref] = 0
                    dic_headers[ref] = complete_header
                else:
                    dic_size[ref] += len(l.rstrip())           
        return dic_size, dic_headers

    def _get_md5(self, fasta:str) -> str:
        hasher = hashlib.md5()
        with open(fasta, "rb") as f:
            buf = f.read()
            hasher.update(buf)
        fasta_md5 = hasher.hexdigest()
        return fasta_md5

    def removeGenomeFromGenomeAndTaxon(self, fasta: str, name: str, taxid: int = None, gcf: str = None, acc: str = None): 
        """Remove a genome entry from genome and taxon collection. If the corresponding taxon contain only this genome, it is removed too. If not, it's just updated.
        
        :param fasta: Path to fasta file
        :type fasta: str
        :param name: Name of the taxon
        :type name: str
        :param taxid: NCBI taxid, defaults to None
        :type taxid: int, optional
        :param gcf: Gcf assembly accession number, defaults to None
        :type gcf: str, optional
        :param acc: Accession number, defaults to None
        :type acc: str, optional
        :raises error.ConsistencyError: Raise if their is problem of consistency between genome and taxon collection
        :return: Deleted genome id
        :rtype: str
        """
        logging.info(f"= Remove genome\nfasta: {fasta}\n name : {name}\n taxid : {taxid}\n gcf : {gcf}\n acc : {acc}")
        try :
            fasta_md5 = fastaHash(fasta)
        except FileNotFoundError:
            logging.error(f"Can't remove your entry because fasta file is not found.")
            return  

        try: 
            genome = self.genomedb.get(fasta_md5, gcf, acc)
        except error.DuplicateError as e:
            logging.error(f"Can't remove your entry because of DuplicateError\nreason: {e}")
            return
        except error.ConsistencyError as e :
            logging.error(f"Can't remove your entry because of ConsistencyError\nreason: {e}")
            return
        
        if not genome:
            logging.error(f"Genome doesn't exist in genome database")
            return

        logging.info(f"Genome : {genome._id}")
        logging.info(f"Corresponding taxon is {genome.taxon}")

        taxon = self.taxondb.getFromID(genome.taxon)
        if not taxon: 
            raise error.ConsistencyError(f"Associated taxon {genome.taxon} doesn't exist in taxon database")
        
        if taxon.taxid != taxid : 
            raise error.ConsistencyError(f"Database taxon taxid {taxon.taxid} doesn't correspond to your taxid {taxid}")

        if taxon.name != name : 
            raise error.ConsistencyError(f"Database taxon name {taxon.name} doesn't correspond to your name {name}")

        if not genome._id in taxon.genomeColl: 
            raise error.ConsistencyError(f"Genome {genome._id} is not linked with its taxon {taxon._id}")

        taxon.genomeColl.remove(genome._id)

        if not taxon.genomeColl:
            logging.info(f"Your genome was the only version of the taxon (name : {taxon.name}, taxid : {taxon.taxid}). Taxon will be deleted.")
            genome.remove()
            taxon.remove()

        else: 
            logging.info(f"Delete this version of Taxon (name : {taxon.name}, taxid : {taxon.taxid}). Current version become the previous one.")
            taxon.current = taxon.genomeColl[-1]
            genome.remove()
            taxon.store()
        
        return genome._id 

    def createTree(self):
        """Create taxonomic tree from taxon database. Will automatically search taxon, create tree and store tree into database.
        Use engine.taxonomic_tree to create and format tree. 
        """
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
            "fields": ["taxid", "name", "current"],
            "limit":total_taxons
        }
        try:
            docs = self.taxondb.execute_mango_query(mango_query)
        except error.MangoQueryError as e:
            logging.error(f"MangoQueryError\n{e}")
            return

        logging.info(f"{len(docs)} taxids found")
        dic_taxid = {d["taxid"]:{"name": d["name"], "uuid": d["current"]} for d in docs}

        mango_query = {
            "selector": {
                "taxid" : {"$eq": None}
            },
            "fields": ["name", "current"],
            "limit": total_taxons
        }
        try:
            docs = self.taxondb.execute_mango_query(mango_query)
        except error.MangoQueryError as e:
            logging.error(f"MangoQueryError\n{e}")
            return

        logging.info(f"{len(docs)} other taxon found")
        dic_others = {d["name"]:{"uuid": d["current"]} for d in docs }


        if not dic_taxid and not dic_others:
            logging.error(f"No taxid found in {self.taxondb.db_name}")
            return

        # Create tree
        tree = tTree.create_tree(dic_taxid, dic_others)
        tree_json = tree.get_json()
        
        tree_entity = self.treedb.createNewTree(tree_json)
        tree_entity.store()
        
 
    def addFastaMotifs(self, fastaFileList, batchSize=10000, indexLocation=None, cacheLocation=None):
        for fastaFile in fastaFileList:
            sgRNA_data, uuid, ans = self.addFastaMotif(fastaFile, batchSize)
            if cacheLocation and not ans is None:
                fPickle = cacheLocation + "/" + uuid + ".p"
                pickle.dump(sgRNA_data, open(fPickle, "wb"), protocol=3)
                logging.info(f"databaseManager::addFastaMotif:pickling of \"{len(sgRNA_data.keys())}\" sgnRNA motifs wrote to {fPickle}")
       
            if indexLocation:
                indexLen = self.addIndexMotif(indexLocation, sgRNA_data, uuid)
                logging.info(f"databaseManager::addFastaMotif:indexation of \"{indexLen}\" sgnRNA motifs wrote to {indexLocation}/{uuid}.index")
       
    def addIndexMotif(self, location, sgnRNAdata, uuid):
        indexData, wLen = computeMotifsIndex(sgnRNAdata)
        dataLen = sgRNAIndexWriter(indexData, f"{location}/{uuid}.index", wLen, getEncoding()[0])
        #with open (location + '/' + uuid + '.index', 'w') as fp:
        #    fp.write(str(len(indexData)) + "\n")
        #    for datum in indexData:
        #        fp.write( ' '.join([str(d) for d in datum]) + "\n")
        return dataLen

    def addFastaMotif(self, fastaFile, batchSize):   
        fasta_md5 = fastaHash(fastaFile)
        genomeEntity = self.getGenomeEntity(fasta_md5)
        if not genomeEntity:
            raise error.NoGenomeEntity(fastaFile)
        uuid = genomeEntity._id
        sgRNA_data = sgRNAfastaSearch(fastaFile, uuid)
        allKeys = list(sgRNA_data.keys())
        if not self.wrapper.hasKeyMappingRules:
            logging.warn(f"databaseManager::addFastaMotif:Without mapping rules, {len(allKeys)} computed sgRNA motifs will not be inserted into database")
            return (sgRNA_data, uuid, None)
        
        logging.info(f"databaseManager::addFastaMotif:Slicing \"{uuid}\" to volDocAdd its {len(allKeys)} genomic sgRNA motif")       
        for i in range(0,len(allKeys), batchSize):
          
            j = i + batchSize if i + batchSize < len(allKeys) else len(allKeys)
            legit_keys = allKeys[i:j]
            d = { k : sgRNA_data[k] for k in legit_keys }
            logging.info(f"databaseManager::addFastaMotif:Attempting to volDocAdd {len(d.keys())} sets sgRNA keys")
       
            r = self.wrapper.volDocAdd(d)
        return (sgRNA_data, uuid, r)
    
    def addHeadersAndFastaName(self, fasta, gcf = None, acc = None):
        """Create for hack update to v2 format. Will update entries in genome collection to add fasta complete headers and fasta name.
        
        :param fasta: Path to fasta file
        :type fasta: str
        :param gcf: gcf assembly accession
        :type gcf: str
        :param acc: accession number
        :type acc: str
        """
        fasta_name = fasta.split("/")[-1]
        try :
            fasta_md5 = fastaHash(fasta)
        except FileNotFoundError:
            logging.error(f"Can't add your entry because fasta file is not found.")
            return       

        try:
            genome_entity = self.genomedb.get(fasta_md5, gcf, acc)
        except error.DuplicateError as e: 
            logging.error(f"Can't add your entry because DuplicateError in genome database \nReason : \n{e}")
            return
        except error.ConsistencyError as e: 
            logging.error(f"Can't add your entry because ConsistencyError in genome database \nReason : \n{e}")
            return

    def _getAllIdsMotifs(self, motif_ranks:str) -> Set[str]:
        motifs_ids = set()
        with open(motif_ranks) as mr : 
            motifs_json = json.load(mr)
        for species in motifs_json["ranks"]:
            motifs_ids.add(species["specie"])
        return motifs_ids 

    def getAllIdsFromDatabase(self, database:str, motif_ranks:Optional[str] = None) -> Optional[Set[str]]:
        """Get all ids from collection whatever collection is
        
        :param database: database to proceed
        :type database: str (motif | genome | index | blast)
        :param motif_ranks: path to json ranked species provided by ms-db-manager node service, defaults to None
        :type motif_ranks: Optional[str], optional
        :return: set of ids present in database
        :rtype: Optional[Set[str]]
        """
        if database == "motif":
            if not motif_ranks:
                raise Exception("For motif database, you need to provide motif ranks previously computed")
            
            return self._getAllIdsMotifs(motif_ranks)
        
        if database == "genome" :
           return self.genomedb.all_ids
        
        if database == "blast":
            if not self.blastdb : 
                raise Exception("Blast database doesn't exist. Do you provide path in config ?")
                
            return self.blastdb.all_ids

        if database == "index":
            if not self.indexdb:
                raise Exception("Index database doesn't exit. Do you provide path in config ?")
            return self.indexdb.all_ids

        return   
        
    def checkConsistency(self, db1: str, db2: str, motif_ranks:Optional[str] = None, metadata_out:Optional[str] = None) -> Tuple[Set[str], Set[str]]:
        """Check consistency between genome collection ids and motifs collection ids. For now, motif ids has to be provided as a json file, but maybe later we include the computation here.

        :param motif_ranks: Path to json file for motif collection (from ms-db-manager)
        :type motif_ranks: str
        :param metadata_out: Path to tsv file where to write metadata for ids in genome and not in motif
        :type metadata_out: str
        :param db1: First database to check
        :type db1: str among motif | genome | blast | index
        :param db2: Second database to check
        :type db2: str among motif | genome | blast | index
        :return: Tuple of 2 sets, first with ids present in db1 and not in db2, second with ids present in db2 and not in db1
        :rtype: Tuple[Set[str], Set[str]]
        """
        logging.info(f" = Check database consistency between {db1} and {db2}")

        ids_db1 = self.getAllIdsFromDatabase(db1, motif_ranks)
        ids_db2 = self.getAllIdsFromDatabase(db2, motif_ranks)

        logging.info(f"{len(ids_db1)} in {db1} database")
        logging.info(f"{len(ids_db2)} in {db2} database")
        
        #For id present in genome and not in motif, also display metadata

        in_db1 = ids_db1.difference(ids_db2)
        in_db2 = ids_db2.difference(ids_db1)
        write_metadata = False
        if db1 == "genome":
            write_metadata = True
            write_data = in_db1
        elif db2 == "genome":
            write_metadata = True
            write_data = in_db2
        
        if write_metadata:
            out = open(metadata_out, "w")
            out.write("#fasta\ttaxid\tname\tgcf\taccession\n")
            for id in write_data:
                genome = self.genomedb.getFromID(id)
                taxon = self.taxondb.getFromID(genome.taxon)
                out.write(f'{genome.fasta_name}\t{taxon.taxid}\t{taxon.name}\t{genome.gcf_assembly if genome.gcf_assembly else "-"}\t{genome.accession_number if genome.accession_number else "-"}\n')
            out.close()
            
        return in_db1, in_db2

    def removeFromBlast(self, fastaList: List[str]):
        """Remove entries from blast database from fasta files
        
        :param fastaList: List of paths to fasta files
        :type fastaList: List[str]
        """
        logging.info("Remove from Blast database")
        self.blastdb.set_remove_mode(True)
        for zFasta in fastaList:
            fasta_md5 = fastaHash(zFasta)
            genomElem = self.genomedb.get(fasta_md5)
            if not genomElem:
                logging.error(f"{zFasta} is not stored in genome database")
                return
            for header, seq, _id  in zFastaReader(zFasta):
                _header = f">{genomElem._id}|{header.replace(r'/^>//', '')}"
                self.blastdb.remove(_header, seq)
        self.blastdb.close()











        
        

