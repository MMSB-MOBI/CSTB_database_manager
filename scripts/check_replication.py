#!/software/mobi/crispr-manager/2.0.0/bin/python

import argparse, re, time
import pycouch.wrapper as couch_wrapper


def args_gestion():
    parser = argparse.ArgumentParser(description = "Check for databases if the same backup exists")
    parser.add_argument("--prefix", metavar="<str>", help = "Collection prefix")
    parser.add_argument("--url", metavar="<str>", help = "couchDB endpoint", required = True)
    args = parser.parse_args()
    args.url = args.url.rstrip("/")
    return args   

def get_sizes(db_names):
    dic_size = {}
    for name in db_names:
        res = couchDB.couchGetRequest(name)
        if "doc_count" in res:
            dic_size[name] = res["doc_count"]
        elif "error" in res and res["error"] == "not_found":
            dic_size[name] = 0
        else:
            raise Exception(f"Error for getRequest on {name}. Response is {res}")
    return dic_size

if __name__ == "__main__":
    ARGS = args_gestion()

    couchDB = couch_wrapper.Wrapper(ARGS.url)
    if not couchDB.couchPing():
        raise Exception("Can't ping database")


    db_names = [name for name in couchDB.couchDbList() if name.startswith(ARGS.prefix)]
    original_dbs = set([name.split("-")[0] for name in db_names])

    db_sizes = get_sizes(original_dbs)
    correct_backup = []
    incorrect_backup = []

    TIMESTAMP = time.strftime("%Y%m%d-%H%M%S")
    detailed_output = f"{TIMESTAMP}-detailed_replication.tsv"
    o = open(detailed_output, "w")
    o.write("Original database\tBackup database\tStatus\tOriginal size\tBackup size\n")

    for db in db_sizes:
        correct = False
        backups_names = [name for name in db_names if name.split("-")[0] == db and "-" in name]
        for back in backups_names:
            backup_size = couchDB.couchGetRequest(back)["doc_count"]
            if backup_size in [db_sizes[db] -1, db_sizes[db], db_sizes[db] + 1]: #Hack that works even if we have a view document. Needs to find a better way to do this !
                correct = True
                o.write(f"{db}\t{back}\tcorrect\t{db_sizes[db]}\t{backup_size}\n")
            else:
                o.write(f"{db}\t{back}\tincorrect\t{db_sizes[db]}\t{backup_size}\n")
        
        if correct:
            correct_backup.append(db)
        else:
            incorrect_backup.append(db)

    o.close()
    
    print("== Correct backup")
    print("\n".join(correct_backup))
    print("== Incorrect backup")       
    print("\n".join(incorrect_backup))    
    print(f"Detailed information in {detailed_output}") 
    