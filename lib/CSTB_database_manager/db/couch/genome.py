from typeguard import typechecked
from typing import TypedDict, Optional, Dict
import CSTB_database_manager.db.couch.virtual
import CSTB_database_manager.utils.error as error

GenomeDoc = TypedDict("GenomeDoc", {"_id": str, "_rev": Optional[str], "taxon": str, "fasta_md5": str, "gcf_assembly": Optional[str], "accession_number": Optional[str], "size": Dict, "date": str}, total=False)

@typechecked
class GenomeDB(CSTB_database_manager.db.couch.virtual.Database):
    """Handle genome database
    """
    def __init__(self, wrapper, db_name):
        super().__init__(wrapper, db_name)

    def get(self, fasta_md5:str, gcf: str = None, acc: str = None) -> Optional['GenomeEntity']:
        """Get genome entity from couchdb from fasta_md5 and optional parameters gcf number and accession number and create Genome Entity
        
        :raises error.DuplicateError: Raises if fasta is duplicated in database
        :raises error.ConsistencyError: Raises if fasta exists in database but gcf or accession number don't correspond.
        :return: Genome entity or None
        :rtype: Optiona[GenomeEntity]
        """
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

    def createNewGenome(self, fasta_md5:str, size: Dict, gcf: str = None, acc: str = None) -> 'GenomeEntity':
        """Create genome entity
        
        :return: Genome Entity
        :rtype: GenomeEntity
        """
        doc = {
            "_id": self.wrapper.couchGenerateUUID(),
            "fasta_md5" : fasta_md5,
            "gcf_assembly" : gcf, 
            "accession_number": acc,
            "size": size
        }
        return GenomeEntity(self, doc)

    def getFromID(self, id: str) -> Optional['GenomeEntity']:
        """Get genome couch document from couch _id and create GenomeEntity
        
        :return: Genome Entity object or None
        :rtype: Optional[GenomeEntity]
        """
        doc = self.wrapper.couchGetDoc(self.db_name, id)
        if doc:
            return GenomeEntity(self, doc)
        return None

@typechecked
class GenomeEntity(CSTB_database_manager.db.couch.virtual.Entity):
    """Object that represents couch entry for a genome
    
    :ivar fasta_md5: md5 for fasta sequence
    :vartype fasta_md5: str
    :ivar taxon: taxon uuid
    :vartype taxon: Optional[str]
    :ivar gcf_assembly: gcf assembly accession
    :vartype gcf_assembly: str
    :ivar accession_number: accession number
    :vartype accession_number: str
    :ivar size: Sizes of fasta sequences
    :vartype size: Dict -> {fasta_header(str):size(int)}
    
    """

    def __init__(self, container: 'GenomeDB', couchDoc: 'GenomeDoc'):
        """init function
        
        :param container: Container to handle database
        :type container: GenomeDB
        :param couchDoc: couch json document
        :type couchDoc: GenomeDoc
        """
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
