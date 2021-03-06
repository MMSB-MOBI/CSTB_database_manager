from typeguard import typechecked
from typing import TypedDict, Optional, List
import CSTB_database_manager.db.couch.virtual
import CSTB_database_manager.db.couch.genome as genomeDB
import CSTB_database_manager.utils.error as error

TaxonDoc = TypedDict("TaxonDoc", 
{"_id": str, "_rev": str, "genomeColl": List[str], "name": str, "taxid": Optional[int], "current": str, "date": str}, 
total=False)

class TaxonDB(CSTB_database_manager.db.couch.virtual.Database):
    """Handle taxon database
    """
    def __init__(self, wrapper, db_name):
        super().__init__(wrapper, db_name)

    def get(self, name: str, taxid: int = None):
        if taxid: 
            taxid_mango_query = {
                 "selector": {
                     "taxid" : taxid
                 }
            }
            doc = self.wrapper.couchPostDoc(self.db_name + "/_find", taxid_mango_query)
            if not doc['docs']:
                return None
            
            if len(doc['docs']) > 1:
                raise error.DuplicateError(f'{taxid} exists {len(doc["docs"])} times in taxon database')
            
            # Check if name corresponds
            doc = doc['docs'][0]
            if doc["name"] != name:
                raise error.ConsistencyError(f'{taxid} exists in taxon database but associated with an other name (name : {doc["name"]}). Update taxon if you want to insert.')
            return TaxonEntity(self, doc)

        name_mango_query = {
            "selector": {
                "name": name
            }
        }

        doc = self.wrapper.couchPostDoc(self.db_name + "/_find", name_mango_query)
        
        if not doc['docs']:
            return None

        if len(doc['docs']) > 1:
            raise error.DuplicateError(f'{name} exists {len(doc["docs"])} times in taxon database associated with no taxid')
        
        return TaxonEntity(self,doc['docs'][0])
    
    #def create_insert_doc(self, name: str, taxid: int = None) -> TaxonDoc:
    #    return {
    #        "_id" : self.wrapper.couchGenerateUUID(),
    #        "name": name,
    #         "taxid": taxid         
    #    }

    def createNewTaxon(self, name: str, taxid: int = None) -> 'TaxonEntity':
        doc = {
            "_id": self.wrapper.couchGenerateUUID(),
            "name" : name,
            "taxid" : taxid
        }
        return TaxonEntity(self, doc)
    
    def add(self, doc: TaxonDoc) -> CSTB_database_manager.db.couch.virtual.PositivePutAnswer:
        return self.wrapper.couchAddDoc(doc, target = self.db_name, key = doc["_id"])

    def getFromID(self, id: str) -> Optional['TaxonEntity']:
        doc = self.wrapper.couchGetDoc(self.db_name, id)
        if doc:
            return TaxonEntity(self, doc)
        return None

    def getGenomesUuid(self):
        genomes_ids = set()
        taxon_ids = self.all_ids
        for t_id in self.all_ids:
            taxon = self.getFromID(t_id)
            genomes_ids.update(taxon.genomeColl)
        return genomes_ids



@typechecked
class TaxonEntity(CSTB_database_manager.db.couch.virtual.Entity):
    """Represent an entry in taxon database"""
    def __init__(self, container : 'TaxonDB', couchDoc: TaxonDoc):
        super().__init__(container, couchDoc)
        self.name = couchDoc["name"]
        self.taxid = couchDoc["taxid"]
        self.current = couchDoc["current"] if couchDoc.get("current") else None
        self.genomeColl = couchDoc["genomeColl"] if couchDoc.get("genomeColl") else []
    
    def __eq__(self, other : 'TaxonEntity') -> bool:
        return self.taxid == other.taxid and self.name == other.name and self.current == other.current and self.genomeColl == other.genomeColl

    


        



