from typeguard import typechecked
from typing import TypedDict, Optional, List

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

    def get(self, name: str, taxid: int = None) -> Optional[TaxonDoc]:
        mango_query = {
            "selector": {
                "name": name, "taxid": taxid
            }
        }

        doc = self.wrapper.couchPostDoc(self.db_name + "/_find", mango_query)
        if not doc['docs']:
            return None
        return doc['docs'][0]
    
    def create_insert_doc(self, name: str, taxid: int = None) -> TaxonDoc:
        return {
            "_id" : self.wrapper.couchGenerateUUID(),
            "name": name,
            "taxid": taxid         
        }
    
    def add(self, doc: TaxonDoc) -> PositivePutAnswer:
        return self.wrapper.couchAddDoc(doc, target = self.db_name, key = doc["_id"])
        

    
