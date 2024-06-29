"""Allowed values for metadata structures

Available lists
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


# Names of metadata entities allowed to be in the metadata structure
ENTITIES = [
    'namespaces',
    'schemas',
    'systems',
    'data_entities', 
    'pipelines',
    'dag_config'
]



# Keys from metadata that are to be toplevel keys in a flat instance
## Pipelines
FLAT_PL_KEYS = [
    'id', 
    'unid', 
    'namespace', 
    'name', 
    'type', 
    'version', 
    'scope', 
    'velocity'
]

## Entities
FLAT_ENT_KEYS = [
    'id', 
    'unid', 
    'namespace', 
    'name', 
    'type', 
    'interface', 
    'entity_schema'
]

## Systems
FLAT_SYS_KEYS = [
    'id', 
    'unid', 
    'namespace', 
    'name', 
    'type'
]



# Reserved prefixes that keys in a config dict in entities, systems and 
# pipelines can NOT have
RESERVED_CONFIG_KEY_PREFIXES = [
    'pl_',
    'ent_',
    'sys_'
]



# Schema allowed types
SCHEMA_TYPES = [
    'avro',
    'bigquery'
]

# System allowed types
SYSTEM_TYPES = [
    'external',
    'internal',
    'platform'
]

# Data_entities allowed types
DATA_ENTITY_ALLOWED_TYPES = [
    'datasource',
    'dataset'
]

# Data_entities allowed interfaces
DATA_ENTITY_ALLOWED_INTERFACES = [
    'api_rest',
    'api_graphql',
    'sql',
    'google_cloud_storage'
]

# Pipeline allowed scopes
PIPELINE_ALLOWED_SCOPES = [
    'single',
    'compound'
]

# Pipeline allowed types
PIPELINE_ALLOWED_TYPES = [
    'ingest',
    'transform',
    'delivery'
]

# Pipeline allowed velocity
PIPELINE_ALLOWED_VELOCITY = [
    'batch',
    'streaming'
]