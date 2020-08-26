#!/software/mobi/crispr-manager/2.0.0/bin/python
import pycouch.wrapper as couch_wrapper
import argparse, re

def args_gestion():
    parser = argparse.ArgumentParser(description = "Delete couchDB database")
    parser.add_argument("--db", metavar="<str>", help = "Replicate database(s) corresponding to this regular expression", required=True)
    parser.add_argument("--url", metavar="<str>", help = "couchDB endpoint", required = True)

    args = parser.parse_args()
  
    args.url = args.url.rstrip("/")
    return args  

def get_regexp(input_str):
    regexp = "^" + input_str.replace("*", ".*") + "$"
    return re.compile(regexp)

if __name__ == "__main__":
    ARGS = args_gestion()

    couchDB = couch_wrapper.Wrapper(ARGS.url)
    if not couchDB.couchPing():
        raise Exception("Can't ping database")

    db_names = couchDB.couchDbList()

    if ARGS.db:
        regExp = get_regexp(ARGS.db)
        db_names = [db_name for db_name in db_names if regExp.match(db_name)]

    if not db_names:
        print("No database to delete")
        exit()
    
    print("== Databases to delete:")
    for db_name in db_names:
        print(db_name)

    confirm = input("Do you want to delete this databases ? (y/n) ")
    while (confirm != "y" and confirm != "n"):
        confirm = input("I need y or n answer : ") 

    if confirm == "n":
        exit()

    for db_name in db_names:
        res = couchDB.couchDeleteRequest(db_name)
        print(db_name, res)
