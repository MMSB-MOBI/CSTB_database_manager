import os, glob

def connect(index_folder):
    if not os.path.isdir(index_folder):
        raise error.IndexConnectionError("Index directory doesn't exist.")

    return IndexDB(index_folder)

class IndexDB():
    def __init__(self, index_db_folder):
        self.database_folder = index_db_folder

    @property
    def all_ids(self):
        return set([f.split("/")[-1].split(".")[0] for f in glob.glob(self.database_folder + "/*.index")])