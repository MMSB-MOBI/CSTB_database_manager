from ete3 import Tree, NCBITaxa, TreeNode
from typeguard import typechecked


@typechecked
class HomemadeTree:
    def __init__(self, ete3_tree):
        self.ete3_tree = ete3_tree
        self.unclassified_branch = None
    
    def create_unclassified_branch(self):
        node = HomemadeNode("unclassified")
        self.unclassified_branch = node
        return self.unclassified_branch

    #Adapted from maxi_tree.py code
    def get_json(self):
        json = {"text": "root"}
        ete3_json = self.__get_json__(self.ete3_tree)
        json["children"] = [ete3_json]
        if self.unclassified_branch: 
            unclassified_json = self.__get_json__(self.unclassified_branch)
            json["children"].append(unclassified_json)
        return json
    
    #From maxi_tree.py code
    def __get_json__(self, node): 
        # Remove the taxon ID if it is present
        json = {"text": node.used_name if hasattr(node, "used_name") else node.sci_name}
        if node.is_leaf():
            json["genome_uuid"] = node.genome_uuid
        # If node has children, create a list of it and traverse it, else do not create attribute
        # children and return the json string
        if node.children:
            json["children"] = []
            for ch in node.children:
                json["children"].append(self.__get_json__(ch))
        return json


@typechecked
class HomemadeNode:
    def __init__(self, name, uuid = None):
        self.used_name = name
        self.genome_uuid = uuid
        self.parent = None
        self.children = []

    def add_children(self, nodes):
        for n in nodes: 
            self.children.append(n)
            n.parent = self

    def is_leaf(self):
        if self.children:
            return True
        return False


def create_tree(dic_taxid, dic_others):
    ncbi = NCBITaxa()
    #Create first tree from taxids
    tree = ncbi.get_topology(list(dic_taxid.keys()))
    for node in tree.iter_descendants():
        if int(node.name) in dic_taxid:
            if node.children:
                node.add_child(name=node.name)
            else:
                node.add_feature("used_name", dic_taxid[int(node.name)]["name"])
                node.add_feature("genome_uuid", dic_taxid[int(node.name)]["uuid"])

    myTree = HomemadeTree(tree)

    #Place taxon with no taxid under a new branch named "others"
    if dic_others:
        unclassified_branch = myTree.create_unclassified_branch()
        unclassified_branch.add_children([HomemadeNode(t, dic_others[t]["uuid"]) for t in dic_others])

    return myTree
    
