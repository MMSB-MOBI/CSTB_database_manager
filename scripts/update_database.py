#!/software/mobi/crispr-manager/2.0.0/bin/python

"""Update database

Usage:
    update_database.py --current <metadata_file> --new <metadata_file>

Options:
    -h --help
    --current <metadata_file> assembly_summary file for current version of database
    --new <metadata_file> assembly_summary file for new version of database
"""

from docopt import docopt

def store_metadata(f): 
    metadata = {}
    f = open(f, "r")
    f.readline()
    for l in f:
        l_split = l.split("\t")
        metadata[l_split[3]] = {"fasta": l_split[0], "taxid": l_split[1], "name" : l_split[2], "accession" : l_split[4], "ftp" : l_split[5]}
    f.close()
    return metadata




if __name__ == "__main__":
    ARGS = docopt(__doc__, version="1.0.0")

    current_metadata = store_metadata(ARGS["--current"])
    new_metadata = store_metadata(ARGS["--new"])

    current_gcf = set(current_metadata.keys())
    new_gcf = set(new_metadata.keys())

    common = current_gcf.intersection(new_gcf)

    print(f"== {len(common)} common genomes")

    news = new_gcf.difference(current_gcf)
    news_sup_version = [n for n in news if int(n.split(".")[1]) > 1]
    
    change_version = []
    for n in news_sup_version:
        short_gcf = n.split(".")[0]
        version = int(n.split(".")[1])
        for i in range(1,version):
            if f"{short_gcf}.{i}" in current_gcf:
                change_version.append({"updated_str" : f"{short_gcf}.{i} => {n}", "gcf":n, "old_gcf":f"{short_gcf}.{i}"}) 
                news.remove(n)

    if change_version:
        o = open("updated_genomes.tsv", "w")
        o.write("#fasta\ttaxid\tname\tgcf\taccession\tftp")
        print(f"== {len(change_version)} updated genomes")
        for elmt in change_version:
            print(elmt["updated_str"])
            gcf_metadata = new_metadata[elmt["gcf"]]
            o.write(f'\n{gcf_metadata["fasta"]}\t{gcf_metadata["taxid"]}\t{gcf_metadata["name"]}\t{elmt["gcf"]}\t{gcf_metadata["accession"]}\t{gcf_metadata["ftp"]}')
        o.close()
        print(f"= updated genomes list in updated_genomes.tsv")
        
    if news:
        o = open("new_genomes.tsv", "w")
        o.write("#fasta\ttaxid\tname\tgcf\taccession\tftp")

        print(f"== {len(news)} new genomes")
        for n in news:
            print(n)
            gcf_metadata = new_metadata[n]
            o.write(f'\n{gcf_metadata["fasta"]}\t{gcf_metadata["taxid"]}\t{gcf_metadata["name"]}\t{n}\t{gcf_metadata["accession"]}\t{gcf_metadata["ftp"]}')
        o.close()
        print(f"= new genomes list in new_genomes.tsv")

    olds = current_gcf.difference(new_gcf)
    for gcf in change_version:
        olds.remove(gcf["old_gcf"])
        
    if olds:
        o = open("deprecated_genomes.tsv", "w")
        o.write("#fasta\ttaxid\tname\tgcf\taccession\tftp")

        print(f"== {len(olds)} deprecated genomes")
        for old in olds:
            print(old)
            gcf_metadata = current_metadata[old]
            o.write(f'\n{gcf_metadata["fasta"]}\t{gcf_metadata["taxid"]}\t{gcf_metadata["name"]}\t{old}\t{gcf_metadata["accession"]}\t{gcf_metadata["ftp"]}')
        o.close()
        print(f"= deprecated genomes list in deprecated_genomes.tsv")
    


