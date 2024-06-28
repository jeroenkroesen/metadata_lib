"""Various constants for metadata lib

Allowed values
---------------
ENTITIES
    Names of metadata entities allowed to be in the metadata structure
FLAT_PL_KEYS
    Keys from a pipeline that are to be toplevel keys in a flat instance
FLAT_ENT_KEYS
    Keys from a data_entity that are to be toplevel keys in a flat instance
FLAT_SYS_KEYS
    Keys from a system that are to be toplevel keys in a flat instance
RESERVED_CONFIG_KEY_PREFIXES
    Reserved prefixes that keys in a config dict in entities, systems and 
    pipelines are NOT allowed have
SCHEMA_TYPES
    Valid values for the 'type' key of a schema
SYSTEM_TYPES
    Valid values for the 'type' key of a system
DATA_ENTITY_ALLOWED_TYPES
    Valid values for the 'type' key of a data_entity
DATA_ENTITY_ALLOWED_INTERFACES
    Valid values for the 'interface' key of a data_entity
PIPELINE_ALLOWED_SCOPES
    Valid values for the 'scope' key of a pipeline
PIPELINE_ALLOWED_TYPES
    Valid values for the 'type' key of a pipeline
PIPELINE_ALLOWED_VELOCITY
    Valid values for the 'velocity' key of a pipeline
"""

from . allowed_values import *