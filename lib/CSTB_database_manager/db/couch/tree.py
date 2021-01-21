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

    def getFromID(self, id:str):
        doc = self.wrapper.couchGetDoc(self.db_name, id)
        if doc:
            return TreeEntity(self, doc)
        return None

@typechecked
class TreeEntity(CSTB_database_manager.db.couch.virtual.Entity):
    def __init__(self, container : 'TreeDB', couchDoc):
        super().__init__(container, couchDoc)
        self.tree = couchDoc["tree"]
        self._tree_object = None

    def __eq__(self, other: 'TreeEntity') -> bool:
        return self.tree == other.tree

    def _load_tree(self):
        self._tree_object = taxonomic_tree.load_tree(self.tree)

    def _rec_count(self, node, current_count):
        current_count += 1

        if not "children" in node:
            return current_count

        for child in node["children"]:
            current_count = self._rec_count(child, current_count)
            
        return current_count


    @property
    def node_numbers(self):
        return self._rec_count(self.tree, 0)
    
        





        