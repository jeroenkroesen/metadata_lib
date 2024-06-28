"""Functions for validation of metadata structures
"""
from copy import deepcopy
from . schemas import (Namespace, Schema, System, Data_entity, Pipeline)
from . definitions import (
    ENTITIES, RESERVED_CONFIG_KEY_PREFIXES, SYSTEM_TYPES, SCHEMA_TYPES,
    DATA_ENTITY_ALLOWED_TYPES, DATA_ENTITY_ALLOWED_INTERFACES,
    PIPELINE_ALLOWED_SCOPES, PIPELINE_ALLOWED_TYPES, PIPELINE_ALLOWED_VELOCITY
)
from . unid import id_to_unid
from . build import create_id_indexes, unid_refs_to_ids

from typing import Union, Dict, List, Any


# Basic support functions

def id_exists(
    metadata_id_idx: dict[ str,list[any] ], entity: str, id: int
) -> bool:
    """Check if an entity of type entity with id id exists in metadata_id_idx

    Parameters
    ----------
    metadata_id_idx : dict[ str,list[any] ]
        Metadata in its by_id form
    entity : str
        The type of entity to check for existence
    id : int
        The id of the entity to check if it exists
    
    Returns
    -------
    bool
        True if the entity exists, False if it does not
    
    Raises
    ------
    ValueError
        If entity is not a valid entity_type.
    """
    if entity not in ENTITIES:
        raise ValueError(f'{entity} is not a valid metadata entity.')
    if id in metadata_id_idx[f'{entity}_idx'].keys():
        return True
    else:
        return False



def unid_exists(
    metadata_unid_idx: dict[ str,list[any] ], entity: str, unid: str
) -> bool:
    """Check if an entity of type entity with unid unid exists in 
        metadata_unid_idx
    
    Parameters
    ----------
    metadata_unid_idx : dict[ str,list[any] ]
        Metadata in its by_unid form.
    entity : str
        The type of entity to check if it exists.
    unid : str
        The unid of the entity to check for existence.
    
    Returns
    -------
    bool
        True if the entity exists, False if it does not

    Raises
    ------
    ValueError
        If entity is not a valid entity_type.
    """
    if entity not in ENTITIES:
        raise ValueError(f'{entity} is not a valid metadata entity.')
    if unid in metadata_unid_idx[f'{entity}_unid_idx'].keys():
        return True
    else:
        return False



def get_next_free_id(
    metadata_id_idx: dict[ str,list[any] ], entity: str
) -> int:
    """Get the next unused id for the given entity type.

    Parameters
    ----------
    metadata_id_idx : dict[ str,list[any] ]
        Metadata in its by_id form,
    entity : str
        THe type of entity to get the available id for.
    
    Returns
    -------
    int
        The first unused id for this type of entity.
    
    Raises
    ------
    ValueError
        If entity is not a valid entity_type.
    """
    if entity not in ENTITIES:
        raise ValueError(f'{entity} is not a valid metadata entity.')
    return max(metadata_id_idx[f'{entity}_idx'].keys()) + 1



def config_keys_are_legal(entity: System | Data_entity | Pipeline) -> bool:
    """Make sure entity does not contain config keys that can conflict with the 
        keys of a flattened instance.
    
    Parameters
    entity : System | Data_entity | Pipeline
        A Pydantic object of System, Data_entity or Pipeline (the entity-types 
        that have config keys)
    
    Returns
    bool
        True if there is no conflict. False if there is a conflict
    """
    if not entity.config:
        return True # No config, no problems
    for key in entity.config.keys():
        if key.startswith('pl_') or key.startswith('ent_') or key.startswith('sys_'):
            return False
    return True





# Validation functions
# Validation for existing entities

def validate_schema(
    schema: Schema,
    metadata_id_idx: dict[ str,list[any] ],
    metadata_unid_idx: dict[ str,list[any] ]
) -> dict[str,any]:
    """Run all validation checks against a given schema

    Perform the following validations against the given schema:
    - The referenced namespace exists
    - The type is an allowed value

    Parameters
    ----------
    schema : Schema
        A Pydantic object of the schema to validate
    metadata_id_idx : dict[ str,list[any] ]
        Metadata in its by_id form
    metadata_unid_idx : dict[ str,list[any] ]
        Metadata in its by_unid form
    
    Returns
    -------
    result : dict[str,any]
        A report with keys:
        - report['valid'] : bool
            True if validation passed. False if validation failed
        - report['errors'] : list[str]
    
    Raises
    ------
    TypeError
        If referenced objects are not ids or unids (int or str)
    """
    result = {
        'valid': True,
        'errors': []
    } # Set result to True until we find an error
    # Referenced namespace exists
    if isinstance(schema.namespace, int):
        if not id_exists(metadata_id_idx, 'namespaces', schema.namespace):
            result['valid'] = False
            result['errors'].append(
                f'Referenced namespace id {schema.namespace} does not exist'
            )
    elif isinstance(schema.namespace, str):
        if not unid_exists(metadata_unid_idx, 'namespaces', schema.namespace):
            result['valid'] = False
            result['errors'].append(
                f'Referenced namespace UNID {schema.namespace} does not exist'
            )
    else:
        emsg = f'Cannot validate schema.namespace if it is not a str or int'
        raise TypeError(emsg)
    # Type is valid
    if not schema.type in SCHEMA_TYPES:
        result['valid'] = False
        error_msg = f'{schema.type} is not a valid type for schemas.'
        error_msg = f'{error_msg} Valid types are:'
        for sch_type in SCHEMA_TYPES:
            error_msg += f' {sch_type},'
        result['errors'].append(error_msg)
    return result



def validate_system(
    system: System,
    metadata_obj: dict[ str,list[any] ],
    metadata_id_idx: dict[ str,list[any] ],
    metadata_unid_idx: dict[ str,list[any] ]
) -> dict[str,any]:
    """Run all checks against a given system

    Perform the following validations against the given system:
    - The referenced namespace exists
    - The type is an allowed value

    Parameters
    ----------
    system : System
        A Pydantic object of the system to validate
    metadata_id_idx : dict[ str,list[any] ]
        Metadata in its by_id form
    metadata_unid_idx : dict[ str,list[any] ]
        Metadata in its by_unid form
    
    Returns
    -------
    result : dict[str,any]
        A report with keys:
        - report['valid'] : bool
            True if validation passed. False if validation failed
        - report['errors'] : list[str]
    
    Raises
    ------
    TypeError
        If references to objects are not ids or unids (int or str)
    """
    result = {
        'valid': True,
        'errors': []
    } # Set result to True until we find an error
    # Referenced namespace exists
    if isinstance(system.namespace, int):
        if not id_exists(metadata_id_idx, 'namespaces', system.namespace):
            result['valid'] = False
            result['errors'].append(
                f'Referenced namespace id {system.namespace} does not exist'
            )
    elif isinstance(system.namespace, str):
        if not unid_exists(metadata_unid_idx, 'namespaces', system.namespace):
            result['valid'] = False
            result['errors'].append(
                f'Referenced namespace UNID {system.namespace} does not exist'
            )
    else:
        emsg = f'Cannot validate system.namespace if it is not a str or int'
        raise TypeError(emsg)
    # Type is valid
    if not system.type in SYSTEM_TYPES:
        result['valid'] = False
        error_msg = f'{system.type} is not a valid type for systems.'
        error_msg = f'{error_msg} Valid types are:'
        for sys_type in SYSTEM_TYPES:
            error_msg += f' {sys_type},'
        result['errors'].append(error_msg)
    return result



def validate_data_entity(
    data_entity: Data_entity,
    metadata_obj: dict[ str,list[any] ],
    metadata_id_idx: dict[ str,list[any] ],
    metadata_unid_idx: dict[ str,list[any] ]
) -> dict[str,any]:
    """Run all checks against a given data_entity

    Perform the following validations against the given data_entity:
    - The referenced namespace exists
    - The referenced system exists
    - The referenced schema exists
    - The type is an allowed value
    - The interface is an allowed value

    Parameters
    ----------
    data_entity : Data_entity
        A Pydantic object of the data_entity to validate
    metadata_id_idx : dict[ str,list[any] ]
        Metadata in its by_id form
    metadata_unid_idx : dict[ str,list[any] ]
        Metadata in its by_unid form
    
    Returns
    -------
    result : dict[str,any]
        A report with keys:
        - report['valid'] : bool
            True if validation passed. False if validation failed
        - report['errors'] : list[str]
    
    Raises
    ------
    TypeError
        If references to objects are not ids or unids (int or str)
    """
    result = {
        'valid': True,
        'errors': []
    } # Set result to True until we find an error
    # Referenced namespace exists
    if isinstance(data_entity.namespace, int):
        if not id_exists(metadata_id_idx, 'namespaces', data_entity.namespace):
            result['valid'] = False
            result['errors'].append(
                f'Referenced namespace id {data_entity.namespace} does not exist'
            )
    elif isinstance(data_entity.namespace, str):
        if not unid_exists(
            metadata_unid_idx, 'namespaces', data_entity.namespace
        ):
            result['valid'] = False
            result['errors'].append(
                f'Referenced namespace UNID {data_entity.namespace} does not exist'
            )
    else:
        emsg = f'Cannot validate data_entity.namespace if it is not a str or int'
        raise TypeError(emsg)
    # Referenced system exists
    if isinstance(data_entity.system, int):
        if not id_exists(metadata_id_idx, 'systems', data_entity.system):
            result['valid'] = False
            result['errors'].append(
                f'Referenced system id {data_entity.system} does not exist'
            )
    elif isinstance(data_entity.system, str):
        if not unid_exists(metadata_unid_idx, 'systems', data_entity.system):
            result['valid'] = False
            result['errors'].append(
                f'Referenced system UNID {data_entity.system} does not exist'
            )
    else:
        emsg = f'Cannot validate data_entity.system if it is not a str or int'
        raise TypeError(emsg)
    # Referenced schema exists
    if isinstance(data_entity.entity_schema, int):
        if not id_exists(metadata_id_idx, 'schemas', data_entity.entity_schema):
            result['valid'] = False
            result['errors'].append(
                f'Referenced schema id {data_entity.entity_schema} does not exist'
            )
    elif isinstance(data_entity.entity_schema, str):
        if not unid_exists(
            metadata_unid_idx, 'schemas', data_entity.entity_schema
        ):
            result['valid'] = False
            result['errors'].append(
                f'Referenced schema UNID {data_entity.entity_schema} does not exist'
            )
    else:
        emsg = f'Cannot validate data_entity.entity_schema if it is not a str or int'
        raise TypeError(emsg)
    # Type is valid
    if not data_entity.type in DATA_ENTITY_ALLOWED_TYPES:
        result['valid'] = False
        error_msg = f'{data_entity.type} is not a valid type for data_entities.'
        error_msg = f'{error_msg} Valid types are:'
        for de_type in DATA_ENTITY_ALLOWED_TYPES:
            error_msg += f' {de_type},'
        result['errors'].append(error_msg)
    # Interface is valid
    if not data_entity.interface in DATA_ENTITY_ALLOWED_INTERFACES:
        result['valid'] = False
        error_msg = f'{data_entity.interface} is not a valid interface for data_entities.'
        error_msg = f'{error_msg} Valid interfaces are:'
        for ent_interface in DATA_ENTITY_ALLOWED_INTERFACES:
            error_msg += f' {ent_interface},'
        result['errors'].append(error_msg)
    return result



def validate_pipeline(
    pipeline: Pipeline,
    metadata_obj: dict[ str,list[any] ],
    metadata_id_idx: dict[ str,list[any] ],
    metadata_unid_idx: dict[ str,list[any] ]
) -> dict[str,any]:
    """Run all checks against a given pipeline

    Perform the following validations against the given pipeline:
    - The referenced namespace exists
    - Allreferenced data_entities in the input_output key exist
    - The type is an allowed value
    - The scope is an allowed value
    - The velocity is an allowed value

    Parameters
    ----------
    pipeline : Pipeline
        A Pydantic object of the pipeline to validate
    metadata_obj : dict[ str,list[any] ]
        Metadata in its md_objects form
    metadata_id_idx : dict[ str,list[any] ]
        Metadata in its by_id form
    metadata_unid_idx : dict[ str,list[any] ]
        Metadata in its by_unid form
    
    Returns
    -------
    result : dict[str,any]
        A report with keys:
        - report['valid'] : bool
            True if validation passed. False if validation failed
        - report['errors'] : list[str]
    
    Raises
    ------
    TypeError
        If references to objects are not ids or unids (int or str)
    """
    result = {
        'valid': True,
        'errors': []
    } # Set result to True until we find an error

    # Referenced namespace exists
    if isinstance(pipeline.namespace, int):
        if not id_exists(metadata_id_idx, 'namespaces', pipeline.namespace):
            result['valid'] = False
            result['errors'].append(
                f'Referenced namespace id {pipeline.namespace} does not exist'
            )
    elif isinstance(pipeline.namespace, str):
        if not unid_exists(metadata_unid_idx, 'namespaces', pipeline.namespace):
            result['valid'] = False
            result['errors'].append(
                f'Referenced namespace UNID {pl_unid} does not exist'
            )
    else:
        emsg = f'Cannot validate pipeline.namespace if it is not a str or int'
        raise TypeError(emsg)
    
    # Entities referenced in input_output exist
    # Iterate instances
    for instance in pipeline.input_output:
        # Proces input
        if isinstance(instance['input'], list):
            # List style: iterate multiple entities
            for inent in instance['input']:
                if isinstance(inent, int):
                    if not id_exists(metadata_id_idx, 'data_entities', inent):
                        result['valid'] = False
                        emsg = f'Input entity {inent} does not exist.'
                        result['errors'].append(emsg)
                elif isinstance(inent, str):
                    if not unid_exists(
                        metadata_unid_idx, 'data_entities', inent
                    ):
                        result['valid'] = False
                        emsg = f'Input entity {inent} does not exist.'
                        result['errors'].append(emsg)
                else:
                    emsg = f'Cannot validate input entity if it is not a str or int'
                    raise TypeError(emsg)
        else:
            # Single input
            if isinstance(instance['input'], int):
                if not id_exists(
                    metadata_id_idx, 'data_entities', instance['input']
                ):
                    result['valid'] = False
                    emsg = f'Input entity {instance["input"]} does not exist.'
                    result['errors'].append(emsg)
            elif isinstance(instance['input'], str):
                if not unid_exists(
                    metadata_unid_idx, 'data_entities', instance['input']
                ):
                    result['valid'] = False
                    emsg = f'Input entity {instance["input"]} does not exist.'
                    result['errors'].append(emsg)
            else:
                emsg = f'Cannot validate input entity if it is not a str or int'
                raise TypeError(emsg)
        # Proces output
        if isinstance(instance['output'], list):
            # List style: iterate multiple entities
            for outent in instance['output']:
                if isinstance(outent, int):
                    if not id_exists(metadata_id_idx, 'data_entities', outent):
                        result['valid'] = False
                        emsg = f'Output entity {outent} does not exist.'
                        result['errors'].append(emsg)
                elif isinstance(outent, str):
                    if not unid_exists(
                        metadata_unid_idx, 'data_entities', outent
                    ):
                        result['valid'] = False
                        emsg = f'Output entity {outent} does not exist.'
                        result['errors'].append(emsg)
                else:
                    emsg = f'Cannot validate output entity if it is not a str or int'
                    raise TypeError(emsg)
        else:
            # Single output
            if isinstance(instance['output'], int):
                if not id_exists(
                    metadata_id_idx, 'data_entities', instance['output']
                ):
                    result['valid'] = False
                    emsg = f'Output entity {instance["output"]} does not exist.'
                    result['errors'].append(emsg)
            elif isinstance(instance['output'], str):
                if not unid_exists(
                    metadata_unid_idx, 'data_entities', instance['output']
                ):
                    result['valid'] = False
                    emsg = f'Output entity {instance["output"]} does not exist.'
                    result['errors'].append(emsg)
            else:
                emsg = f'Cannot validate output entity if it is not a str or int'
                raise TypeError(emsg)

    # Type is valid
    if not pipeline.type in PIPELINE_ALLOWED_TYPES:
        result['valid'] = False
        error_msg = f'{pipeline.type} is not a valid type for pipelines.'
        error_msg = f'{error_msg} Valid types are:'
        for pl_type in PIPELINE_ALLOWED_TYPES:
            error_msg += f' {pl_type},'
        result['errors'].append(error_msg)

    # Scope is valid
    if not pipeline.scope in PIPELINE_ALLOWED_SCOPES:
        result['valid'] = False
        error_msg = f'{pipeline.scope} is not a valid scope for pipelines.'
        error_msg = f'{error_msg} Valid types are:'
        for pl_scope in PIPELINE_ALLOWED_SCOPES:
            error_msg += f' {pl_scope},'
        result['errors'].append(error_msg)

    # Velocity is valid
    if not pipeline.velocity in PIPELINE_ALLOWED_VELOCITY:
        result['valid'] = False
        error_msg = f'{pipeline.velocity} is not a valid velocity for pipelines.'
        error_msg = f'{error_msg} Valid types are:'
        for pl_velocity in PIPELINE_ALLOWED_VELOCITY:
            error_msg += f' {pl_velocity},'
        result['errors'].append(error_msg)

    return result





# Additional validation for new or edited entities

def validate_new_namespace(
    namespace: Namespace,
    metadata_id_idx: dict[ str,list[any] ],
    metadata_unid_idx: dict[ str,list[any] ]
) -> dict[str,any]:
    """Run all checks against given namespace. 

    Perform the following validations against the given namespace:
    - id is unique
    - unid is unique

    Parameters
    ----------
    namespace : Namespace
        A Pydantic object of the namespace to validate
    metadata_id_idx : dict[ str,list[any] ]
        Metadata in its by_id form
    metadata_unid_idx : dict[ str,list[any] ]
        Metadata in its by_unid form
    
    Returns
    -------
    result : dict[str,any]
        A report with keys:
        - report['valid'] : bool
            True if validation passed. False if validation failed
        - report['errors'] : list[str]
    
    Raises
    ------
    TypeError
        If references to objects are not ids or unids (int or str)
    """
    result = {
        'valid': True,
        'errors': []
    } # Set result to True until we find an error
    # Unique UNID:
    if unid_exists(metadata_unid_idx, 'namespaces', namespace.name):
        result['valid'] = False
        result['errors'].append(
            f'A namespace with name {namespace.name} exists already'
        )
    # Unique id:
    if id_exists(metadata_id_idx, 'namespaces', namespace.id):
        result['valid'] = False
        result['errors'].append(
            f'A namespace with id {namespace.id} exists already'
        )
    return result



def validate_new_schema(
    schema: Schema,
    metadata_id_idx: dict[ str,list[any] ],
    metadata_unid_idx: dict[ str,list[any] ]
) -> dict[str,any]:
    """Run all checks against a given schema

    Perform the following validations against the given schema:
    - The referenced namespace exists
    - The type is an allowed value
    - id is unique
    - unid is unique

    Parameters
    ----------
    schema : Schema
        A Pydantic object of the schema to validate
    metadata_id_idx : dict[ str,list[any] ]
        Metadata in its by_id form
    metadata_unid_idx : dict[ str,list[any] ]
        Metadata in its by_unid form
    
    Returns
    -------
    result : dict[str,any]
        A report with keys:
        - report['valid'] : bool
            True if validation passed. False if validation failed
        - report['errors'] : list[str]
    
    Raises
    ------
    TypeError
        If referenced objects are not ids or unids (int or str)
    """
    # Run default validations
    result = validate_schema(
        schema,
        metadata_id_idx,
        metadata_unid_idx
    )
    if not result['valid']:
        # Errors already. Return without further tests
        return result
    # Unique id:
    if id_exists(metadata_id_idx, 'schemas', schema.id):
        result['valid'] = False
        result['errors'].append(
            f'A schema with id {schema.id} exists already'
        )
    # Unique UNID:
    schema_unid = f'{schema.name}.{schema.version}'
    if unid_exists(
        metadata_unid_idx, 'schemas', schema_unid
    ):
        result['valid'] = False
        result['errors'].append(
            f'A schema with UNID {schema_unid} exists already'
        )
    return result



def validate_new_system(
    system: System,
    metadata_obj: dict[ str,list[any] ],
    metadata_id_idx: dict[ str,list[any] ],
    metadata_unid_idx: dict[ str,list[any] ]
) -> dict[str,any]:
    """Run all checks against a given system

    Perform the following validations against the given system:
    - The referenced namespace exists
    - The type is an allowed value
    - id is unique
    - unid is unique

    Parameters
    ----------
    system : System
        A Pydantic object of the system to validate
    metadata_id_idx : dict[ str,list[any] ]
        Metadata in its by_id form
    metadata_unid_idx : dict[ str,list[any] ]
        Metadata in its by_unid form
    
    Returns
    -------
    result : dict[str,any]
        A report with keys:
        - report['valid'] : bool
            True if validation passed. False if validation failed
        - report['errors'] : list[str]
    
    Raises
    ------
    TypeError
        If references to objects are not ids or unids (int or str)
    """
    # Run default validations
    result = validate_system(
        system,
        metadata_obj,
        metadata_id_idx,
        metadata_unid_idx
    )
    if not result['valid']:
        # Errors already. Return without further tests
        return result
    # Unique id:
    if id_exists(metadata_id_idx, 'systems', system.id):
        result['valid'] = False
        result['errors'].append(
            f'A system with id {system.id} exists already'
        )
    # Unique UNID:
    ## Rebuild id_idx with new system included to be able to use unid generation 
    ## functions.
    tmp_sys = unid_refs_to_ids(
        metadata_unid_idx=metadata_unid_idx,
        obj=deepcopy(system)
    )
    tmp_obj = deepcopy(metadata_obj)
    tmp_obj['systems'].append(tmp_sys)
    tmp_id_idx = create_id_indexes(metadata_obj=tmp_obj)
    system_unid = id_to_unid(tmp_id_idx, 'systems', tmp_sys.id)
    if unid_exists(
        metadata_unid_idx, 'systems', system_unid
    ):
        result['valid'] = False
        result['errors'].append(
            f'A system with UNID {system_unid} exists already'
        )
    return result



def validate_new_data_entity(
    data_entity: Data_entity,
    metadata_obj: dict[ str,list[any] ],
    metadata_id_idx: dict[ str,list[any] ],
    metadata_unid_idx: dict[ str,list[any] ]
) -> dict[str,any]:
    """Run all checks against a given data_entity

    Perform the following validations against the given data_entity:
    - The referenced namespace exists
    - The referenced system exists
    - The referenced schema exists
    - The type is an allowed value
    - The interface is an allowed value
    - id is unique
    - unid is unique

    Parameters
    ----------
    data_entity : Data_entity
        A Pydantic object of the data_entity to validate
    metadata_id_idx : dict[ str,list[any] ]
        Metadata in its by_id form
    metadata_unid_idx : dict[ str,list[any] ]
        Metadata in its by_unid form
    
    Returns
    -------
    result : dict[str,any]
        A report with keys:
        - report['valid'] : bool
            True if validation passed. False if validation failed
        - report['errors'] : list[str]
    
    Raises
    ------
    TypeError
        If references to objects are not ids or unids (int or str)
    """
    # Run standard validations
    result = validate_data_entity(
        data_entity,
        metadata_obj,
        metadata_id_idx,
        metadata_unid_idx
    )
    if not result['valid']:
        # Errors already. Return without further tests
        return result
    # Unique id:
    if id_exists(metadata_id_idx, 'data_entities', data_entity.id):
        result['valid'] = False
        result['errors'].append(
            f'A data_entity with id {data_entity.id} exists already'
        )
    # Unique UNID:
    ## Rebuild id_idx with new data_entity included to be able to use  
    ## unid generation functions.
    tmp_ent = unid_refs_to_ids(
        metadata_unid_idx=metadata_unid_idx,
        obj=deepcopy(data_entity)
    )
    tmp_obj = deepcopy(metadata_obj)
    tmp_obj['data_entities'].append(tmp_ent)
    tmp_id_idx = create_id_indexes(metadata_obj=tmp_obj)
    data_entity_unid = id_to_unid(tmp_id_idx, 'data_entities', data_entity.id)
    if unid_exists(
        metadata_unid_idx, 'data_entities', data_entity_unid
    ):
        result['valid'] = False
        result['errors'].append(
            f'A data_entity with UNID {data_entity_unid} exists already'
        )
    return result



def validate_new_pipeline(
    pipeline: Pipeline,
    metadata_obj: dict[ str,list[any] ],
    metadata_id_idx: dict[ str,list[any] ],
    metadata_unid_idx: dict[ str,list[any] ]
) -> dict[str,any]:
    """Run all checks against a given pipeline

    Perform the following validations against the given pipeline:
    - The referenced namespace exists
    - Allreferenced data_entities in the input_output key exist
    - The type is an allowed value
    - The scope is an allowed value
    - The velocity is an allowed value
    - id is unique
    - unid is unique

    Parameters
    ----------
    pipeline : Pipeline
        A Pydantic object of the pipeline to validate
    metadata_obj : dict[ str,list[any] ]
        Metadata in its md_objects form
    metadata_id_idx : dict[ str,list[any] ]
        Metadata in its by_id form
    metadata_unid_idx : dict[ str,list[any] ]
        Metadata in its by_unid form
    
    Returns
    -------
    result : dict[str,any]
        A report with keys:
        - report['valid'] : bool
            True if validation passed. False if validation failed
        - report['errors'] : list[str]
    
    Raises
    ------
    TypeError
        If references to objects are not ids or unids (int or str)
    """
    # Run standard validations
    result = validate_pipeline(
        pipeline,
        metadata_obj,
        metadata_id_idx,
        metadata_unid_idx
    )
    if not result['valid']:
        # Errors already. Return without further tests
        return result
    # Unique id:
    if id_exists(metadata_id_idx, 'pipelines', pipeline.id):
        result['valid'] = False
        result['errors'].append(
            f'A pipeline with id {pipeline.id} exists already'
        )
    # Unique UNID:
    ## Rebuild id_idx with new data_entity included to be able to use  
    ## unid generation functions.
    tmp_pl = unid_refs_to_ids(
        metadata_unid_idx=metadata_unid_idx,
        obj=deepcopy(pipeline)
    )
    tmp_obj = deepcopy(metadata_obj)
    tmp_obj['pipelines'].append(tmp_pl)
    tmp_id_idx = create_id_indexes(metadata_obj=tmp_obj)
    pl_unid = id_to_unid(tmp_id_idx, 'pipelines', pipeline.id)
    if unid_exists(
        metadata_unid_idx, 'pipelines', pl_unid
    ):
        result['valid'] = False
        result['errors'].append(
            f'A pipeline with UNID {pl_unid} exists already'
        )
    return result



def get_namespace_dependants(
    namespace: Namespace,
    metadata_obj: dict[ str,list[any] ]
) -> dict[ str, bool | list[any]]:
    """Return a list of entity objects that depend on this namespace

    Parameters
    ----------
    namespace : Namespace
        The namespace to find dependants for.
    metadata_obj : dict[ str,list[any] ]
        Metadata in its md_objects form
    
    Returns
    -------
    result : dict[ str, bool | list[any]]
        The result report with the following keys:
        - result['has_dependants'] : bool
            True if dependants where found, False in case of no dependants.
        - result['dependants'] : list[any]
            A list of objects that depend on this namespace
    """
    result = {
        'has_dependants': False,
        'dependants': []
    }
    for ent_type, ent_list in metadata_obj.items():
        if ent_type != 'namespaces':
            for ent in ent_list:
                if ent.namespace == namespace.id:
                    result['has_dependants'] = True
                    result['dependants'].append(ent)
    return result



def get_schema_dependants(
    schema: Schema,
    metadata_obj: dict[ str,list[any] ]
) -> dict[ str, bool | list[any] ]:
    """Return a list of entity objects that depend on this schema

    Parameters
    ----------
    schema : Schema
        The schema to find dependants for.
    metadata_obj : dict[ str,list[any] ]
        Metadata in its md_objects form
    
    Returns
    -------
    result : dict[ str, bool | list[any]]
        The result report with the following keys:
        - result['has_dependants'] : bool
            True if dependants where found, False in case of no dependants.
        - result['dependants'] : list[any]
            A list of objects that depend on this schema
    """
    result = {
        'has_dependants': False,
        'dependants': []
    }
    for ent in metadata_obj['data_entities']:
        if ent.entity_schema == schema.id:
            result['has_dependants'] = True
            result['dependants'].append(ent)
    return result


def get_system_dependants(
    system: System,
    metadata_obj: dict[ str,list[any] ]
) -> dict[ str, bool | list[any] ]:
    """Return a list of entity objects that depend on this system
    
    Parameters
    ----------
    system : System
        The system to find dependants for.
    metadata_obj : dict[ str,list[any] ]
        Metadata in its md_objects form
    
    Returns
    -------
    result : dict[ str, bool | list[any]]
        The result report with the following keys:
        - result['has_dependants'] : bool
            True if dependants where found, False in case of no dependants.
        - result['dependants'] : list[any]
            A list of objects that depend on this system
    """
    result = {
        'has_dependants': False,
        'dependants': []
    }
    for ent in metadata_obj['data_entities']:
        if ent.system == system.id:
            result['has_dependants'] = True
            result['dependants'].append(ent)
    return result


def get_data_entity_dependants(
    data_entity: Data_entity,
    metadata_obj: dict[ str,list[any] ]
) -> dict[ str, bool | list[any] ]:
    """Return a list of entity objects that depend on this data_entity

    Parameters
    ----------
    data_entity : Data_entity
        The data_entity to find dependants for.
    metadata_obj : dict[ str,list[any] ]
        Metadata in its md_objects form
    
    Returns
    -------
    result : dict[ str, bool | list[any]]
        The result report with the following keys:
        - result['has_dependants'] : bool
            True if dependants where found, False in case of no dependants.
        - result['dependants'] : list[any]
            A list of objects that depend on this data_entity
    """
    result = {
        'has_dependants': False,
        'dependants': []
    }
    for pl in metadata_obj['pipelines']:
        # Make sure pl is not already in results
        if pl in result['dependants']:
            continue
        # Iterate the pipelines instances
        for instance in pl.input_output:
            if isinstance(instance['input'], list):
                # Input is a list, iterate it's entities
                for inent in instance['input']:
                    if inent == data_entity.id:
                        result['has_dependants'] = True
                        result['dependants'].append(pl)
            else:
                # input is an int. Assign
                if instance['input'] == data_entity.id:
                    result['has_dependants'] = True
                    result['dependants'].append(pl)
            if isinstance(instance['output'], list):
                # Output is a list. Iterate it
                for outent in instance['output']:
                    if outent == data_entity.id:
                        result['has_dependants'] = True
                        result['dependants'].append(pl)
            else:
                # Outent is an int. Assign
                if instance['output'] == data_entity.id:
                    result['has_dependants'] = True
                    result['dependants'].append(pl)
    return result



def get_entity_dependants(
    entity: Namespace | Schema | System | Data_entity,
    metadata_obj: dict[ str,list[any] ]
) -> dict[ str, bool | list[any] ]:
    """Return a list of entity objects that depend on this entity

    Parameters
    ----------
    entity : Namespace | Schema | System | Data_entity
        The object to find dependants for.
    metadata_obj : dict[ str,list[any] ]
        Metadata in its md_objects form
    
    Returns
    -------
    result : dict[ str, bool | list[any]]
        The result report with the following keys:
        - result['has_dependants'] : bool
            True if dependants where found, False in case of no dependants.
        - result['dependants'] : list[any]
            A list of objects that depend on this object
    
    Raises
    ------
    TypeError
        In case passed entity is not of type Namespace, Schema, System or 
        Data_entity
    """
    if isinstance(entity, Namespace):
        return get_namespace_dependants(entity, metadata_obj)
    elif isinstance(entity, Schema):
        return get_schema_dependants(entity, metadata_obj)
    elif isinstance(entity, System):
        return get_system_dependants(entity, metadata_obj)
    elif isinstance(entity, Data_entity):
        return get_data_entity_dependants(entity, metadata_obj)
    else:
        raise TypeError(
            f'get_entity_dependants cannot proces object of type {type(entity).__name__}'
        )