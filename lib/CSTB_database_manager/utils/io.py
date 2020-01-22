import hashlib
def tsvReader(tsvFilePath, _min=None, _max=None):
    with open(tsvFilePath) as f : 
        f.readline()
        i = 0
        for l in f:            
            l_split = l.strip("\n").split("\t")
            if len(l_split) != 5:
                raise ValueError(f"Current tsv record length is not 5 ({len(l_split)})\n=>{l_split}") 
            fasta = l_split[0]
            if not fasta:
                raise FormatError("genomes list, first column (fasta) is empty")  
            taxid = l_split[1]
            if  taxid == '-':
                taxid = None
            else:
                taxid = int(taxid)
            name = l_split[2]
            if not name:
                raise FormatError("genomes list, third column (name) is empty")
            gcf = l_split[3]
            if  gcf == '-':
                gcf = None
            acc = l_split[4]
            if acc == '-':
                acc = None

            t = (fasta, name, taxid, gcf, acc)           
            if not _min is None:
                if not _max is None:
                    if i >= _min and i <= _max:                       
                        yield (t) 
            elif not _max is None:
                if i <= _max:
                    yield (t) 
            else : 
                yield (t)
            i += 1
    
def fileHash(filePath):
    hasher = hashlib.md5()
    with open(filePath, "rb") as f:
        buf = f.read()
        hasher.update(buf)
        return hasher.hexdigest()