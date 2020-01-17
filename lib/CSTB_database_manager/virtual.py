class Entity():
    def __init__(self, container, couchDoc):
        self.container = container
        self._id = couchDoc["_id"]
        self._rev = couchDoc["_rev"] if couchDoc.get("_rev") else None

    def __str__(self):
        return str(self.__dict__)

    def isInDB(self) -> bool:
        if self._rev:
            return True
        return False


    @property
    def couchDoc(self):
        doc = self.__dict__
        del doc["container"]
        if not self._rev:
            del doc["_rev"]
        return doc

    def store(self):
        db_entry = self.container.getFromID(self._id)
        if not self == db_entry:
           print("Add document")
           self.container.add(self.couchDoc)



        