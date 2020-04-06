import CSTB_database_manager.utils.error as error
from CSTB_core.utils.io import Zfile as zFile

from CSTB_core.utils.io import hashStripedString as hashSequence
from CSTB_core.utils.io import fileHash
import os, glob, re, pickle
from CSTB_core.utils.io import which
from CSTB_core.utils.io import gunzipToFile as gunzip
from CSTB_core.utils.io import fileToGunzip as gzip
from CSTB_core.utils.io import zFastaReader

from subprocess import check_call
import logging

MAX_COUNT=25

# check formatdb availability
def connect(blastFolder):
    if not which("makeblastdb"):
        raise error.BlastConnectionError("Executable makeblastdb is missing")
    if not os.path.isdir(blastFolder):
        raise error.BlastConnectionError("Blast directory doesn't exist.")
    return BlastDB(blastFolder)

class BlastDB ():
    def __init__(self, blastFolder, compressed=False):
        self.fastaAsArchive = compressed # By Default we will reject fasta under gz b/c I/O too slow        
        self.location = blastFolder
        self.registry = self._parsingDatabase()
        self._buffer = []
        self._delete_buffer = []
        if self.empty:
            logging.warn(f"Database {self.tag} seems empty")
            self.data = {}
            self.checksum = None
            return

        logging.info(f"blastdb::_parsingDatabase:Computing checksum for {self.fastaFile}...")
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

    @property
    def all_ids(self):
        return set([header.split("|")[0].lstrip(">") for header in self.data.values()])
    
    def _indexDump(self):    
        fDump = f"{self.location}/{self.tag}.pkl"
        _ = {
            'checksum' : self.checksum,
            'data' : self.data
        }
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
        fastaName = f"{self.location}/*.mfasta"
        if self.fastaAsArchive:
            fastaName += ".gz"
        fastaFile = [ f for f in glob.glob(fastaName) ]
        if len(fastaFile) > 1:
            msg = (
                f"Unexpected  number of "
                f"number or ${fastaName} file "
                f"({len(fastaFile)}) found in blastFolder"
            )
            raise error.BlastConnectionError(msg)
        
        if not fastaFile:
            return None
        
        return fastaFile[0]

    def _setRegistry(self):
        filesRegistry = {
                    'pkl' : None,
                    'nhr' : [],
                    'nin' : [],
                    'nsq' : [],
                    'build' : { 
                        'err' : None,
                        'log' : None
                    }
                }

        tag = self.tag
        _reRegularFile =  f"{tag}(\.[\d]+){{0,1}}"
        _reLogFile = f"{tag}_build"
        
        for file in glob.glob(f"{self.location}/*.*"):
            if re.search(rf'/{tag}.mfasta(.gz){{0,1}}$', file):
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
                #raise error.BlastConnectionError(f"Unregistred extension for file {filename} =>{file_extension}")
                logging.warn(f'Warning: Unregistred extension for file {filename} =>{file_extension}')
                continue
            if not type(_root[file_extension]) is list and not _root[file_extension] is None: 
                raise error.BlastConnectionError(f"Previous instance of {file_extension} registred" )
            elif type(_root[file_extension]) is list:       
                _root[file_extension].append(filename)
            else:
                _root[file_extension] = f"{filename}.{file_extension}"

        for k in ['nhr', 'nin', 'nsq']:
            filesRegistry[k] = sorted(filesRegistry[k]) 

        for k in ['nin', 'nsq']:
            if not filesRegistry['nhr'] == filesRegistry[k]:
                raise error.BlastConnectionError(f"Uneven sets of blast database file pin/{k}")

        return filesRegistry

    def _restoreIndex(self, filePickle):
        logging.info(f"Restoring index from {filePickle}")
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
            
        logging.info(f"Building index on {self.fastaFile}, "
              f" this may take a while...")
        data = {}
        with zFile(self.fastaFile) as handle:
            for genome_seqrecord in SeqIO.parse(handle, "fasta"):
                genome_seq = genome_seqrecord.seq
                header = genome_seqrecord.id
                _id = hashSequence(str(genome_seq))
                data[_id] = header
        
        logging.info(f"{len(data.keys())} fasta records successfully indexed")
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
            logging.info(f"blast::add:Bouncing {header}, sequence already stored")
            return
        self._buffer.append( (header, sequence, key) )

    def remove(self, header:str, sequence:str): 
        """Remove sequence from blast database
        
        :param header: Sequence header
        :type header: str
        :param sequence: Nucleotide sequence
        :type sequence: str
        """
        key = hashSequence(sequence)
        if not key in self.data: 
            logging.warn(f"blast::remove:Fasta sequence {header} doesn't exist in blast database")
        else:
            self._delete_buffer.append( (header, sequence, key) )


    def _add_to_mfasta(self, overwrite = False):
        """Add sequence in buffer to the multifasta. They can be add at the end of the multifasta or the multifasta can be rewrite.
        
        :param overwrite: overwrite multifasta or not, defaults to False
        :type overwrite: bool, optional
        """
        if overwrite:
            logging.info("Overwrite mfasta")
            fp = open (self.fastaBufferFile, 'w')
        else:
            fp = open(self.fastaBufferFile, 'a')

        for t in self._buffer:
            fp.write(t[0] + '\n')
            fp.write(t[1] + '\n')
        
        fp.close()

    def _remove_from_mfasta(self):
        """Parse current multifasta and just keep sequence that are not in _delete_buffer in _buffer. The _buffer sequences can be then rewrite in a multifasta.
        """
        to_delete_headers = [buf[0] for buf in self._delete_buffer]
        for header, seq, _id  in zFastaReader(self.fastaBufferFile):
            key = hashSequence(seq)
            _header = f">{header}"
            if not _header in to_delete_headers: 
                self._buffer.append( (_header, seq, key) ) # Add sequences that we want to keep to buffer for rewriting

    def flush(self):
        logging.info("flushing")
        self.fastaBufferFile = None
        # No previsous fasta record
        if not self.fastaFile:           
            self.fastaBufferFile = f"{self.location}/{self.tag}.mfasta"
            self.fastaFile =  self.fastaBufferFile
        # Previous record is ziped
        elif self.fastaAsArchive :
            self.fastaBufferFile = gunzip(self.fastaFile)
        # Previous record is flat
        else :
            self.fastaBufferFile = self.fastaFile
        
        remove = False 
        if self._delete_buffer:
            remove = True
            self._remove_from_mfasta()

        logging.info(f"{len(self._delete_buffer)} sequences to delete")
        logging.info(f"{len(self._buffer)} sequences to add or keep")
        self._add_to_mfasta(overwrite = remove) 
        
    def clean(self):
        logging.info(f"Computing checksum of {self.fastaFile}")
        self.checksum  = os.path.getsize(self.fastaFile)
        logging.info(f"Cleaning")                
        if self.fastaAsArchive:
            logging.info(f"Zipping main fasta record")
            self.fastaFile = gzip(self.fastaBufferFile)
            logging.info(f"Deleting main fasta record {self.fastaBufferFile}")
            os.remove(self.fastaBufferFile)

    def clean_registry(self):
        """Clean blast registry by deleting all files
        """
        logging.info("No sequence are conserved, delete all blast files")
        all_files = glob.glob(self.location + "/*")
        for f in all_files:
            os.remove(f)
    
    def _formatdb(self):
        stdRootPath = f"{self.location}/{self.tag}_build"
        args = ['makeblastdb', '-in', self.fastaBufferFile, '-dbtype', 'nucl', '-out', f"{self.location}/{self.tag}"]
        #formatdb -t $DATABASE_TAG -i $MFASTA -l ${DATABASE_TAG}_build.log -o T -n $DATABASE_TAG
        with open(f"{stdRootPath}.log", 'a') as stdout:
            with open(f"{stdRootPath}.err", 'a') as stderr:
                logging.info(f"Running {args}")
                check_call(args, stdout=stdout, stderr=stderr, cwd=self.location)
    
    def updateIndex(self):
        for header, seq, hKey in self._delete_buffer:
            del self.data[hKey]
        for header, seq, hKey in self._buffer:
            self.data[hKey] = header

    def close(self):
        logging.info("closing")
        if not self._buffer and not self._delete_buffer:
            return

        if self._delete_buffer and set(self.data.values()) == set([buf[0] for buf in self._delete_buffer]):
            self.clean_registry()
            return

        self.flush()
        self._formatdb()
        self.clean()
        self.updateIndex()
        logging.info(f"blastDB::close: inserting {len(self._buffer)} fasta records before closing")
        fName = self._indexDump() 
        logging.info(f"Index wrote to {fName}")
        