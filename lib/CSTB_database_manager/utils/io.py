import hashlib, gzip, os, re

# Expected format

#fasta   taxid   name    gcf     accession  ftp


def tsvReader(tsvFilePath, _min=None, _max=None):
    with open(tsvFilePath) as f : 
        f.readline()
        i = 0
        for l in f:            
            l_split = list( filter(lambda x:  x != '', l.strip("\n").split("\t") ) )
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

''' ZIP and straight  OPEN delivers SIMILAR MD5
>>> import gzip
>>> import hashlib
>>> fp = gzip.open('test.gz', 'rb')
>>> hasher = hashlib.md5()
>>> buf = fp.read()
>>> hasher.update(buf)
>>> hasher.hexdigest()
'a02b540693255caec7cf9412da86e62f'

>>> import hashlib
>>> fp = open('test.txt', 'rb')
>>> hasher = hashlib.md5()
>>> buf = fp.read()
>>> hasher.update(buf)
>>> hasher.hexdigest()
'a02b540693255caec7cf9412da86e62f'
'''
## Compute hash on fasta striping: header-line, spaces, and new-line
def fileHash(filePath):
    hasher = hashlib.md5()
    with Zfile(filePath, 'rb') as f:
        f.readline()# discard header
        buf = str(f.read(), 'utf-8')# convert bte to string for striping               
        buf = re.sub( r'\s+', '',  buf)
        buf = buf.encode('utf-8')# encode for md5 hash
        hasher.update(buf)
    return hasher.hexdigest()

class Zfile(object):
    def __init__(self, filePath, mode='r'):
        self.file_obj = zOpen(filePath, mode)
    def __enter__(self):
        return self.file_obj
    def __exit__(self, type, value, traceback):
        self.file_obj.close()

# return a stream using base name of gzip extension:
def zOpen(filePath, mode='r'):
    m = mode
    mz = 'rt' if mode == 'r' else 'rb'
    try: 
        fp = open(filePath, m)
        return fp
    except (OSError, IOError) as e:
        fp = gzip.open(filePath + '.gz', mz)
        return fp


def zExists(filePath):
    return os.path.isfile(filePath) or os.path.isfile(filePath + '.gz')