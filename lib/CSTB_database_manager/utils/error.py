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