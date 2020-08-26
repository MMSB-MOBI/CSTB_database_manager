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

def get_backups(db_names, wrapper):
    dic_backup = {}
    for name in db_names:
        pref = name.split("-")
        if len(pref) == 1:
            dic_backup[name] = wrapper.couchGetRequest(name)["doc_count"]
    return dic_backup 

if __name__ == "__main__":
    ARGS = args_gestion()

    couchDB = couch_wrapper.Wrapper(ARGS.url)
    if not couchDB.couchPing():
        raise Exception("Can't ping database")

    db_names = [name for name in couchDB.couchDbList() if name.startswith(ARGS.prefix)]

    db_sizes = get_backups(db_names, couchDB)
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
            if backup_size == db_sizes[db]:
                correct = True
                o.write(f"{db}\t{back}\tcorrect\t{db_sizes[db]}\t{backup_size}\n")
            else:
                o.write(f"{db}\t{back}\tuncorrect\t{db_sizes[db]}\t{backup_size}\n")
        
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
    