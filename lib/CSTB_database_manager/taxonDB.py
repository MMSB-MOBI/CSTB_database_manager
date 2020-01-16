from typeguard import typechecked
from typing import TypedDict, Optional, List
import CSTB_database_manager.virtual
import CSTB_database_manager.genomeDB as genomeDB

TaxonDoc = TypedDict("TaxonDoc", 
{"_id": str, "_rev": str, "genomeColl": List[str], "name": str, "taxid": Optional[int], "current": str}, 
total=False)

class PositivePutAnswer(TypedDict):
    ok : bool
    id: str
    rev: str

@typechecked
class TaxonDB():
    def __init__(self, wrapper, db_name):
        self.wrapper = wrapper
        self.db_name = db_name

    def get(self, name: str, taxid: int = None) -> Optional['TaxonEntity']:
        mango_query = {
            "selector": {
                "name": name, "taxid": taxid
            }
        }

        doc = self.wrapper.couchPostDoc(self.db_name + "/_find", mango_query)
        if not doc['docs']:
            return None
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
    
    def add(self, doc: TaxonDoc) -> PositivePutAnswer:
        return self.wrapper.couchAddDoc(doc, target = self.db_name, key = doc["_id"])

    def getFromID(self, id: str) -> Optional['TaxonEntity']:
        doc = self.wrapper.couchGetDoc(self.db_name, id)
        if doc:
            return TaxonEntity(self, doc)
        return None

    


@typechecked
class TaxonEntity(CSTB_database_manager.virtual.Entity):
    def __init__(self, container : 'TaxonDB', couchDoc: TaxonDoc):
        super().__init__(container, couchDoc)
        self.name = couchDoc["name"]
        self.taxid = couchDoc["taxid"]
        self.current = couchDoc["current"] if couchDoc.get("current") else None
        self.genomeColl = couchDoc["genomeColl"] if couchDoc.get("genomeColl") else []
    
    def __eq__(self, other : Optional['TaxonEntity']):
        if not other:
            return False
        return self._id == other._id and self._rev == other._rev and self.taxid == other.taxid and self.current == other.current and self.genomeColl == other.genomeColl


        



