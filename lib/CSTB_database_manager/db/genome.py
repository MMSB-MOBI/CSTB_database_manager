from typeguard import typechecked
from typing import TypedDict, Optional, Dict
import CSTB_database_manager.db.virtual
import CSTB_database_manager.utils.error as error


GenomeDoc = TypedDict("GenomeDoc", {"_id": str, "_rev": Optional[str], "taxon": str, "fasta_md5": str, "gcf_assembly": Optional[str], "accession_number": Optional[str], "size": Dict, "date": str}, total=False)

class PositivePutAnswer(TypedDict): #Probably not define this type here, it's in taxonDB too.
    ok : bool
    id: str
    rev: str

@typechecked
class GenomeDB(CSTB_database_manager.db.virtual.Database):
    def __init__(self, wrapper, db_name):
        super().__init__(wrapper, db_name)

    def get(self, fasta_md5:str, gcf: str = None, acc: str = None) -> Optional['GenomeEntity']:
        mango_query = {"selector":
                {"fasta_md5": fasta_md5}
        }
        doc = self.wrapper.couchPostDoc(self.db_name + "/_find", mango_query)

        if not doc['docs']:
            return None

        if len(doc['docs']) > 1 : 
            raise error.DuplicateError(f'Fasta exists {len(doc["docs"])} times in genome database')

        doc = doc['docs'][0]

        gcf_error = False
        acc_error = False
        if gcf and gcf != doc["gcf_assembly"]:
            gcf_error = True

        if acc and acc != doc["accession_number"]:
            acc_error = True

        if gcf_error or acc_error:
            raise error.ConsistencyError(f'Fasta exists in genome database but with an other gcf_assembly and/or accession_number (gcf_assembly: {doc["gcf_assembly"]}, accession_number: {doc["accession_number"]})')

        return GenomeEntity(self, doc)
    
    #def create_insert_doc(self, fasta_md5:str, gcf: str = None, acc: str = None) -> GenomeDoc:
    #    return {
    #        "_id": self.wrapper.couchGenerateUUID(),
    #        "fasta_md5" : fasta_md5,
    #        "gcf_assembly" : gcf, 
    #        "accession_number" : acc
    #    }

    def createNewGenome(self, fasta_md5:str, size: Dict, gcf: str = None, acc: str = None) -> 'GenomeEntity':
        doc = {
            "_id": self.wrapper.couchGenerateUUID(),
            "fasta_md5" : fasta_md5,
            "gcf_assembly" : gcf, 
            "accession_number": acc,
            "size": size
        }
        return GenomeEntity(self, doc)

    #def add(self, doc: GenomeDoc) -> PositivePutAnswer:
    #    return self.wrapper.couchAddDoc(doc, target = self.db_name, key = doc["_id"])

    def getFromID(self, id: str) -> Optional['GenomeEntity']:
        doc = self.wrapper.couchGetDoc(self.db_name, id)
        if doc:
            return GenomeEntity(self, doc)
        return None

@typechecked
class GenomeEntity(CSTB_database_manager.db.virtual.Entity):
    def __init__(self, container: 'GenomeDB', couchDoc: GenomeDoc):
        super().__init__(container, couchDoc)
        self.fasta_md5 = couchDoc["fasta_md5"]
        self.taxon = couchDoc["taxon"] if couchDoc.get("taxon") else None
        self.gcf_assembly = couchDoc["gcf_assembly"] if couchDoc.get("gcf_assembly") else None
        self.accession_number = couchDoc["accession_number"] if couchDoc.get("accession_number") else None
        self.size = couchDoc["size"]

    def __eq__(self, other :'GenomeEntity') -> bool:
        return self.fasta_md5 == other.fasta_md5 and self.taxon == other.taxon and self.gcf_assembly == other.gcf_assembly and self.accession_number == other.accession_number and self.size == other.size
    
    def __str__(self) -> str :
        return str(self.__dict__)
