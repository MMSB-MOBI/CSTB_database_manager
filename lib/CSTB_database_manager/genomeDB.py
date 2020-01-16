from typeguard import typechecked
from typing import TypedDict, Optional
import CSTB_database_manager.virtual
import CSTB_database_manager.error as error


GenomeDoc = TypedDict("GenomeDoc", {"_id": str, "_rev": Optional[str], "taxon": str, "fasta_md5": str}, total=False)

class PositivePutAnswer(TypedDict): #Probably not define this type here, it's in taxonDB too.
    ok : bool
    id: str
    rev: str



@typechecked
class GenomeDB():
    def __init__(self, wrapper, db_name):
        self.wrapper = wrapper
        self.db_name = db_name

    def get(self, fasta_md5: str) -> Optional['GenomeEntity']:

        mango_query = {"selector":
                {"fasta_md5": fasta_md5}
        }
        doc = self.wrapper.couchPostDoc(self.db_name + "/_find", mango_query)

        if not doc['docs']:
            return None

        return GenomeEntity(self, doc['docs'][0])
    
    #def create_insert_doc(self, fasta_md5:str, gcf: str = None, acc: str = None) -> GenomeDoc:
    #    return {
    #        "_id": self.wrapper.couchGenerateUUID(),
    #        "fasta_md5" : fasta_md5,
    #        "gcf_assembly" : gcf, 
    #        "accession_number" : acc
    #    }

    def createNewGenome(self, fasta_md5:str) -> 'GenomeEntity':
        doc = {
            "_id": self.wrapper.couchGenerateUUID(),
            "fasta_md5" : fasta_md5
        }

        return GenomeEntity(self, doc)

    def add(self, doc: GenomeDoc) -> PositivePutAnswer:
        return self.wrapper.couchAddDoc(doc, target = self.db_name, key = doc["_id"])

    def getFromID(self, id: str) -> Optional['GenomeEntity']:
        doc = self.wrapper.couchGetDoc(self.db_name, id)
        if doc:
            return GenomeEntity(self, doc)
        return None



@typechecked
class GenomeEntity(CSTB_database_manager.virtual.Entity):
    def __init__(self, container: 'GenomeDB', couchDoc: GenomeDoc):
        super().__init__(container, couchDoc)
        self.fasta_md5 = couchDoc["fasta_md5"]
        self.taxon = couchDoc["taxon"] if couchDoc.get("taxon") else None

    def __eq__(self, other : Optional['GenomeEntity']):
        if not other:
            return False
        return self._id == other._id and self._rev == other._rev and self.fasta_md5 == other.fasta_md5 and self.taxon == other.taxon

    def store(self):
        db_genome = self.container.getFromID(self._id)
        if not self == db_genome:
           print("Add document")
           self.container.add(self.couchDoc)      