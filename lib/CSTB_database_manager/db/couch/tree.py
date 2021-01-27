from typeguard import typechecked
from typing import TypedDict, Optional, Dict
import CSTB_database_manager.db.couch.virtual
import CSTB_database_manager.engine.taxonomic_tree as taxonomic_tree

#TreeDoc = TypedDict("TreeDoc", {"_id": str, "_rev": str, "date" : str, "tree": Dict}, total=False)

@typechecked
class TreeDB(CSTB_database_manager.db.couch.virtual.Database):
    def __init__(self, wrapper, db_name):
        super().__init__(wrapper, db_name)

    def createNewTree(self, json_tree:Dict) -> 'TreeEntity':
        """Transform json taxonomic tree in TreeEntity
        
        :param json_tree: The json taxonomic tree
        :type json_tree: Dict
        :return: The TreeEntity object corresponding to database entry
        :rtype: TreeEntity
        """
        couch_doc = {
            "_id": "maxi_tree",
            "tree" : json_tree
        }
        return TreeEntity(self, couch_doc)

    def getFromID(self, id):
        doc = self.wrapper.couchGetDoc(self.db_name, id)
        if doc:
            return TreeEntity(self, doc)
        return None

@typechecked
class TreeEntity(CSTB_database_manager.db.couch.virtual.Entity):
    def __init__(self, container : 'TreeDB', couchDoc):
        super().__init__(container, couchDoc)
        self.tree = couchDoc["tree"]
    
        





        