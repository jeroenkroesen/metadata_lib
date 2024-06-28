"""unid.py
    Functions to facilitate working with UNIDs
"""
from . definitions import ENTITIES



def namespace_id_to_unid(
    metadata_id_idx: dict[ str, dict[int,any] ],
    id: int
) -> str:
    """Take an namespace id as integer and return the corresponding unid

    Parameters
    ----------
    metadata_id_idx : dict[ str, dict[int,any] ]
        Metadata in its by_id form
    id : int
        The id of the namespace to get the unid from.
    
    Returns
    -------
    str
        The unid corresponding to the given id
    """
    return metadata_id_idx['namespaces_idx'][id].name



def schema_id_to_unid(
    metadata_id_idx: dict[ str, dict[int,any] ],
    id: int
) -> str:
    """Take a schema id as int and return it's corresponding UNID
        The UNID for schema is: name.version
    
    Parameters
    ----------
    metadata_id_idx : dict[ str, dict[int,any] ]
        Metadata in its by_id form
    id : int
        The id of the schema to get the unid from.
    
    Returns
    -------
    str
        The unid corresponding to the given id
    """
    name = metadata_id_idx['schemas_idx'][id].name
    version = str(metadata_id_idx['schemas_idx'][id].version)
    return f'{name}.{version}'



def system_id_to_unid(
    metadata_id_idx: dict[ str, dict[int,any] ],
    id: int
) -> str:
    """Take a system id as int and return it's corresponding UNID
        The UNID for system is: namespace.name

    Parameters
    ----------
    metadata_id_idx : dict[ str, dict[int,any] ]
        Metadata in its by_id form
    id : int
        The id of the system to get the unid from.
    
    Returns
    -------
    str
        The unid corresponding to the given id
    """
    ns = namespace_id_to_unid(
        metadata_id_idx,
        metadata_id_idx['systems_idx'][id].namespace
    )
    name = metadata_id_idx['systems_idx'][id].name
    return f'{ns}.{name}'



def data_entity_id_to_unid(
    metadata_id_idx: dict[ str, dict[int,any] ],
    id: int
) -> str:
    """Take a data entity id as int and return it's corresponding UNID
        The UNID for data_entity is:
        namespace.systemUNID.name.type
    
    Parameters
    ----------
    metadata_id_idx : dict[ str, dict[int,any] ]
        Metadata in its by_id form
    id : int
        The id of the data_entity to get the unid from.
    
    Returns
    -------
    str
        The unid corresponding to the given id
    """
    ns = namespace_id_to_unid(
        metadata_id_idx,
        metadata_id_idx['data_entities_idx'][id].namespace
    )
    name = metadata_id_idx['data_entities_idx'][id].name
    sys_unid = system_id_to_unid(
        metadata_id_idx, 
        metadata_id_idx['data_entities_idx'][id].system
    )
    ent_type = metadata_id_idx['data_entities_idx'][id].type
    return f'{ns}.{sys_unid}.{name}.{ent_type}'



def pipeline_id_to_unid(
    metadata_id_idx: dict[ str, dict[int,any] ],
    id: int
) -> str:
    """Take a pipeline id as int and return it's corresponding UNID
        The UNID for pipeline is:
        namespace.name.type.version
    
    Parameters
    ----------
    metadata_id_idx : dict[ str, dict[int,any] ]
        Metadata in its by_id form
    id : int
        The id of the pipeline to get the unid from.
    
    Returns
    -------
    str
        The unid corresponding to the given id
    """
    ns = namespace_id_to_unid(
        metadata_id_idx,
        metadata_id_idx['pipelines_idx'][id].namespace
    )
    name = metadata_id_idx['pipelines_idx'][id].name
    pl_type = metadata_id_idx['pipelines_idx'][id].type
    version = str(metadata_id_idx['pipelines_idx'][id].version)
    return f'{ns}.{name}.{pl_type}.{version}'



def id_to_unid(
    metadata_id_idx: dict[ str, dict[int,any] ],
    metadata_entity: str,
    id: int
) -> str:
    """Take an id as integer and return the corresponding unid

    Parameters
    ----------
    metadata_id_idx : dict[ str, dict[int,any] ]
        Metadata in its by_id form
    metadata_entity : str
        The type of metadata entity to get a unid for
    id : int
        The id of the entity to get the unid from.
    
    Returns
    -------
    str
        The unid corresponding to the given id
    """
    if metadata_entity not in ENTITIES:
        raise ValueError(f'{metadata_entity} is not a valid metadata entity.')
        
    # Check given md entity and execute corresponding function
    if metadata_entity == 'namespaces':
        return namespace_id_to_unid(metadata_id_idx, id)
    elif metadata_entity == 'schemas':
        return schema_id_to_unid(metadata_id_idx, id)
    elif metadata_entity == 'systems':
        return system_id_to_unid(metadata_id_idx, id)
    elif metadata_entity == 'data_entities':
        return data_entity_id_to_unid(metadata_id_idx, id)
    elif metadata_entity == 'pipelines':
        return pipeline_id_to_unid(metadata_id_idx, id)
    else:
        raise ValueError(
            f'{metadata_entity} Cannot be processed by id_to_unid()'
        )