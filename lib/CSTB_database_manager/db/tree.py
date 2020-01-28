from typeguard import typechecked
from typing import TypedDict, Optional, Dict
import CSTB_database_manager.db.virtual

#TreeDoc = TypedDict("TreeDoc", {"_id": str, "_rev": str, "date" : str, "tree": Dict}, total=False)

@typechecked
class TreeDB(CSTB_database_manager.db.virtual.Database):
    def __init__(self, wrapper, db_name):
        super().__init__(wrapper, db_name)

    def createNewTree(self, json_tree):
        couch_doc = {
            "_id": "maxi_tree",
            "tree" : json_tree
        }
        return TreeEntity(self, couch_doc)

    def getFromID(self, id: str) -> Optional['TreeEntity']:
        doc = self.wrapper.couchGetDoc(self.db_name, id)
        if doc:
            return TreeEntity(self, doc)
        return None

@typechecked
class TreeEntity(CSTB_database_manager.db.virtual.Entity):
    def __init__(self, container : 'TreeDB', couchDoc):
        super().__init__(container, couchDoc)
        self.tree = couchDoc["tree"]

    def __eq__(self, other: 'TreeEntity') -> bool:
        return self.tree == other.tree


        