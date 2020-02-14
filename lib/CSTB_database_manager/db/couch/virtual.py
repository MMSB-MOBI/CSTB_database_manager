import copy
from datetime import datetime
import pycouch.wrapper as pycouch_wrapper
import CSTB_database_manager.utils.error as error
from typeguard import typechecked
from typing import TypedDict
import logging

class PositivePutAnswer(TypedDict):
    ok : bool
    id: str
    rev: str

@typechecked
class Database():
    """Handle couch database
    
    :ivar wrapper: pycouch wrapper to interrogate couch database
    :vartype wrapper: pycouch.wrapper.Wrapper
    :ivar db_name: database name
    :vartype db_name: str
    """
    def __init__(self, wrapper, db_name):
        self.wrapper = wrapper
        self.db_name = db_name
    
    def remove(self, id):
        logging.info(f"Remove {id} from {self.db_name}")
        try:
            self.wrapper.couchDeleteDoc(self.db_name, id)
        except pycouch_wrapper.CouchWrapperError as e :
            logging.error(f"Can't remove {id} from {self.db_name} database because of CouchWrapperError\n{e}")

    def execute_mango_query(self, mango_query):
        """Execute a mango query in the database
        
        :param mango_query: json dict with mango query
        :type mango_query: Dict
        :raises error.MangoQueryError: Raises if query can't be execute
        :return: List of documents that corresponds to query
        :rtype: List
        """
        try:
            doc = self.wrapper.couchPostDoc(self.db_name + "/_find", mango_query)
        except pycouch_wrapper.CouchWrapperError as e : 
            raise error.MangoQueryError(f"Can't execute mango query because of CouchWrapperError\n{e}")
        
        if not doc['docs']:
            return []

        return doc["docs"]
    
    def number_of_entries(self) -> int:
        """Get number of entries in database
        
        :return: Number of entries in database
        :rtype: int
        """
        doc = self.wrapper.couchGetRequest(self.db_name)
        return doc["doc_count"]

    def add(self, doc) -> PositivePutAnswer:
        """Add a document to database
        
        :param doc: json document to add
        :type doc: Dict
        :return: couch answer
        :rtype: PositivePutAnswer
        """
        return self.wrapper.couchAddDoc(doc, target = self.db_name, key = doc["_id"])

    def replicate(self, new_name:str):
        """Replicate the database
        
        :param new_name: Database will be replicate under this name
        :type new_name: str
        """
        self.wrapper.couchReplicate(self.db_name, new_name)


class Entity():
    """Represents couch entry
    
    :ivar container: Object that handle couch database
    :vartype container: Database
    :ivar _id: _id in couch document
    :vartype _id: str
    :ivar _rev: _rev in couch document
    :ivar date: date in couch document
    :vartype date: str
    """
    def __init__(self, container, couchDoc):
        self.container = container
        self._id = couchDoc["_id"]
        self._rev = couchDoc["_rev"] if couchDoc.get("_rev") else None
        self.date = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

    def __str__(self):
        return str(self.__dict__)

    def isInDB(self) -> bool:
        """Check if entity is already in database
        
        :return: True if entity is in database, False if not
        :rtype: bool
        """
        if self._rev:
            return True
        return False


    @property
    def couchDoc(self):
        """get couch document for the entity
        
        :return: couch document
        :rtype: Dict
        """
        doc = copy.deepcopy(self.__dict__)
        del doc["container"]
        if not self._rev:
            del doc["_rev"]
        return doc

    def store(self):
        """Add entity to database
        """
        db_entry = self.container.getFromID(self._id)
        if not db_entry or self != db_entry:
           logging.info(f"Add document {self._id} in {self.container.db_name}")
           self.container.add(self.couchDoc)
        else:
            logging.warn(f"Same document {self._id} already exists in {self.container.db_name}")

    def remove(self):
        """Remove entity from database
        """
        self.container.remove(self._id)


        