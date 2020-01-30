import CSTB_database_manager.utils.error as error
from CSTB_database_manager.utils.io import Zfile as zFile

from CSTB_database_manager.utils.io import hashStripedString as hashSequence
from CSTB_database_manager.utils.io import fileHash
import os, glob, re, pickle
from CSTB_database_manager.utils.io import which
from CSTB_database_manager.utils.io import gunzipToFile as gunzip
from CSTB_database_manager.utils.io import fileToGunzip as gzip

from subprocess import check_call

MAX_COUNT=25

# check formatdb availability
def connect(blastFolder):
    if not which("formatdb"):
        raise error.BlastConnectionError("Executable formatdb is missing")
    if not os.path.isdir(blastFolder):
        raise error.BlastConnectionError()
    return BlastDB(blastFolder)

class BlastDB ():
    def __init__(self, blastFolder):
        self.location = blastFolder
        self.registry = self._parsingDatabase()
        self._buffer = []
    
        if self.empty:
            print(f"Database {self.tag} seems empty")
            self.data = {}
            self.checksum = None
            return

        print(f"blastdb::_parsingDatabase:Computing checksum for {self.fastaFile}...")
        # HACK TO speed DVL
        #self.checksum = fileHash(self.fastaFile, noHeader=False, stripinSpace=False)
        self.checksum = os.path.getsize(self.fastaFile)
        self.data = self._index()
        if not self.data:
            raise error.BlastConnectionError("indexing Error")
        
        #print(self.registry)        
        #print(f"Data :\n{self.data}")
        
        
    @property
    def empty(self):
        for k in self.registry:
            if k == 'build':
                continue
            if self.registry[k]:
                return False
        return True
    
    def _indexDump(self):    
        fDump = f"{self.location}/{self.tag}.pkl"
        _ = {
            'checksum' : self.checksum,
            'data' : self.data
        }
        print("DONE")
        pickle.dump( _, open( fDump, "wb" ) )
        return fDump
    # Enforcing naming convention
    def _parsingDatabase(self):
        self.tag = self._getTag()
        self.fastaFile = self._getFasta()
        _  = self._setRegistry()
        return _

    def _getTag(self):
        return os.path.basename(self.location)

    def hasTag(self, filePath):
        _  = os.path.basename(filePath)        
        return re.match(f"{self.tag}(\.[\d]+)")

    def _getFasta(self):
        fastaFile = [ f for f in glob.glob(f"{self.location}/*.mfasta.gz") ]
        if len(fastaFile) > 1:
            msg = (
                f"Unexpected  number of "
                f"number or *.fasta.gz file "
                f"({len(fastaFile)}) found in blastFolder"
            )
            raise error.BlastConnectionError(msg)
        
        if not fastaFile:
            return None
        
        return fastaFile[0]

    def _setRegistry(self):
        filesRegistry = {
                    'pkl' : None,
                    'pal' : None,
                    'pin' : [],
                    'psq' : [],
                    'psi' : [],
                    'psd' : [],
                    'phr' : [],
                    'build' : { 
                        'err' : None,
                        'log' : None
                    }
                }

        tag = self.tag
        _reRegularFile =  f"{tag}(\.[\d]+)" + "{0,1}"
        _reLogFile = f"{tag}_build"
        
        for file in glob.glob(f"{self.location}/*.*"):
            if file.endswith(f"/{tag}.mfasta.gz"):
                continue
            filename, file_extension = os.path.splitext(file)
            filename                 = os.path.basename(filename)
            file_extension           = file_extension.replace('.', '')           

            _root = filesRegistry
            if re.match(_reLogFile, filename):
                _root = filesRegistry['build']
            elif not re.match(_reRegularFile, filename):
                raise error.BlastConnectionError(f"Irregular extension found in blast database {file} re/{_reRegularFile}/")
            
    
            if not file_extension in _root:
                raise error.BlastConnectionError(f"Unregistred extension for file {filename} =>{file_extension}")

            if not type(_root[file_extension]) is list and not _root[file_extension] is None: 
                raise error.BlastConnectionError(f"Previous instance of {file_extension} registred" )
            elif type(_root[file_extension]) is list:       
                _root[file_extension].append(filename)
            else:
                _root[file_extension] = f"{filename}.{file_extension}"

        for k in ['pin', 'psq', 'psi', 'psd', 'phr']:
            filesRegistry[k] = sorted(filesRegistry[k]) 

        for k in ['psq', 'psi', 'psd', 'phr']:
            if not filesRegistry['pin'] == filesRegistry[k]:
                raise error.BlastConnectionError(f"Uneven sets of blast database file pin/{k}")
        return filesRegistry

    def _restoreIndex(self, filePickle):
        print(f"Restoring index from {filePickle}")
        with open(filePickle, 'rb') as fp:
            _ = pickle.load(fp)
            checksum = _.get('checksum', None)
            if not checksum:
                raise error.BlastConnectionError(f"No checksum in {filePickle}")
            if not checksum == self.checksum:
                raise error.BlastConnectionError(f"Restored checksum in {filePickle} does not match {self.fastaFile}")
            return  _.get('data', None)

    def _index(self):
        if self.registry['pkl']:
            return self._restoreIndex(f"{self.location}/{self.registry['pkl']}")
            
        print(f"Building index on {self.fastaFile}, "
              f" this may take a while...")
        data = {}
        with zFile(self.fastaFile) as handle:
            for genome_seqrecord in SeqIO.parse(handle, "fasta"):
                genome_seq = genome_seqrecord.seq
                header = genome_seqrecord.id
                _id = hashSequence(str(genome_seq))
                data[_id] = header
        
        print(f"{len(data.keys())} fasta records successfully indexed")
        return data

    def __iter__(self):
        for _id, header  in self.data.items():
            yield(_id, header)

    # Returns True hash(sequence) is part of index
    def get(self, **kwargs):
        if 'seq' in kwargs:
            return self[hashSequence(seq)]
            
    def __getitem__(self, hashKey):
        return self.data[hashKey] if hashKey in self.data else None
    
    def add(self, header, sequence, force=False):
        key = hashSequence(sequence)
        if key in self.data:
            print(f"blast::add:Bouncing {header}, sequence already stored")
            return
        self._buffer.append( (header, sequence, key) )
    
    def flush(self):
        print("flushing")
        self.fastaBufferFile = None
        if not self.fastaFile:           
            self.fastaBufferFile = f"{self.location}/{self.tag}.mfasta"
        else:
            self.fastaBufferFile = gunzip(self.fastaFile)
        
        with open (self.fastaBufferFile, 'a') as fp:
            for t in self._buffer:
                fp.write(t[0] + '\n')
                fp.write(t[1] + '\n')

    def clean(self):
        print(f"Cleaning")                
        print(f"Zipping main fasta record")
        self.fastaFile = gzip(self.fastaBufferFile)
        self.checksum  = os.path.getsize(self.fastaFile)
        os.remove(self.fastaBufferFile)
    
    def _formatdb(self):
        stdRootPath = f"{self.location}/{self.tag}_build"
        args = ['formatdb', '-t', self.tag, '-i', self.fastaBufferFile, '-l', f"{stdRootPath}.log", '-o', 'T', '-n', self.tag]
        #formatdb -t $DATABASE_TAG -i $MFASTA -l ${DATABASE_TAG}_build.log -o T -n $DATABASE_TAG
        with open(f"{stdRootPath}.log", 'a') as stdout:
            with open(f"{stdRootPath}.err", 'a') as stderr:
                print(f"Running {args}")
                check_call(args, stdout=stdout, stderr=stderr, cwd=self.location)
    
    def updateIndex(self):
        for header, seq, hKey in self._buffer:
            self.data[hKey] = header

    def close(self):
        print("closing")
        if not self._buffer :
            return
        self.flush()
        self._formatdb()
        self.clean()
        self.updateIndex()
        print(f"blastDB::close: inserting {len(self._buffer)} fasta records before closing")
        fName = self._indexDump() 
        print(f"Index wrote to {fName}")
        