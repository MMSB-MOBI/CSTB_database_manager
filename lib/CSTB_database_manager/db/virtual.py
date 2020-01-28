import copy
from datetime import datetime
import pycouch.wrapper_class as pycouch_wrapper
import CSTB_database_manager.utils.error as error
from typeguard import typechecked
from typing import TypedDict

class PositivePutAnswer(TypedDict):
    ok : bool
    id: str
    rev: str

@typechecked
class Database():
    def __init__(self, wrapper, db_name):
        self.wrapper = wrapper
        self.db_name = db_name
    
    def remove(self, id):
        print(f"Remove {id} from {self.db_name}")
        try:
            self.wrapper.couchDeleteDoc(self.db_name, id)
        except pycouch_wrapper.CouchWrapperError as e :
            print(f"Can't remove {id} from {self.db_name} database because of CouchWrapperError\n{e}")

    def execute_mango_query(self, mango_query):
        try:
            doc = self.wrapper.couchPostDoc(self.db_name + "/_find", mango_query)
        except pycouch_wrapper.CouchWrapperError as e : 
            raise error.MangoQueryError(f"Can't execute mango query because of CouchWrapperError\n{e}")
        
        if not doc['docs']:
            return []

        return doc["docs"]
    
    def number_of_entries(self) -> int:
        doc = self.wrapper.couchGetRequest(self.db_name)
        return doc["doc_count"]

    def add(self, doc) -> PositivePutAnswer:
        return self.wrapper.couchAddDoc(doc, target = self.db_name, key = doc["_id"])

    def replicate(self, new_name):
        self.wrapper.couchReplicate(self.db_name, new_name)



class Entity():
    def __init__(self, container, couchDoc):
        self.container = container
        self._id = couchDoc["_id"]
        self._rev = couchDoc["_rev"] if couchDoc.get("_rev") else None
        self.date = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

    def __str__(self):
        return str(self.__dict__)

    def isInDB(self) -> bool:
        if self._rev:
            return True
        return False


    @property
    def couchDoc(self):
        doc = copy.deepcopy(self.__dict__)
        del doc["container"]
        if not self._rev:
            del doc["_rev"]
        return doc

    def store(self):
        db_entry = self.container.getFromID(self._id)
        if not db_entry or self != db_entry:
           print(f"Add document {self._id} in {self.container.db_name}")
           self.container.add(self.couchDoc)
        else:
            print(f"Same document already exists.")

    def remove(self):
        self.container.remove(self._id)


        