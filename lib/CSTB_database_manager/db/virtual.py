import copy
from datetime import datetime

class Database():
    def __init__(self, wrapper, db_name):
        self.wrapper = wrapper
        self.db_name = db_name
    
    def remove(self, id):
        print(f"Remove {id} from {self.db_name}")
        try:
            self.wrapper.couchDeleteDoc(self.db_name, id)
        except wrapper.CouchWrapperError as e :
            print(f"Can't remove {id} from {self.db_name} database because of CouchWrapperErro\n{e}")

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

    def remove(self):
        self.container.remove(self._id)


        