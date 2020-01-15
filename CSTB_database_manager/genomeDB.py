from typeguard import typechecked
from typing import TypedDict, Optional

class GenomeDoc(TypedDict):
    #TO DO : add size, GCF, ASM
    _id: str #uuid
    taxon: str #uuid
    fasta_md5: str

class PositivePutAnswer(TypedDict): #Probably not define this type here, it's in taxonDB too.
    ok : bool
    id: str
    rev: str

@typechecked
class GenomeDB():
    def __init__(self, wrapper, db_name):
        self.wrapper = wrapper
        self.db_name = db_name

    def get(self, fasta_md5: str) -> Optional[GenomeDoc]:
        mango_query = {"selector":
                {"fasta_md5": fasta_md5}
        }
        doc = self.wrapper.couchPostDoc(self.db_name + "/_find", mango_query)
        if not doc['docs']:
            return None
        return doc['docs']
    
    def create_insert_doc(self, fasta_md5:str) -> {}:
        return {
            "_id": self.wrapper.couchGenerateUUID(),
            "fasta_md5" : fasta_md5
        }

    
    def add(self, doc: GenomeDoc) -> PositivePutAnswer: #Create the document here and give fasta arg or other function to create doc ? 
        return self.wrapper.couchAddDoc(doc, target = self.db_name, key = doc["_id"])
        
        
