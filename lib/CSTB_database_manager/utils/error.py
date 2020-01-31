class LinkError(Exception):
    pass

class VersionError(Exception):
    pass

class DuplicateError(Exception):
    pass

class ConsistencyError(Exception):
    pass

class NoGenomeEntity(Exception):
    """Raised when a mandatory  get for a genome entity returned None"""
    pass

class MangoQueryError(Exception):
    """Raised when an error occurs while execute mango query"""
    pass

class BlastConnectionError(Exception):
    """Raised when provided fasta file or blast database folder don't exist"""
    pass