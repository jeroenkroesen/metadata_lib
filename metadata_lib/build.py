"""Objects and logic to read and process metadata to useable config structures
"""
from types import SimpleNamespace
from copy import deepcopy
from fastapi.encoders import jsonable_encoder
from . schemas import (Namespace, Schema, System, Data_entity, Pipeline)
from . unid import id_to_unid
from . definitions import ENTITIES, FLAT_PL_KEYS, FLAT_ENT_KEYS, FLAT_SYS_KEYS

from typing import Callable
    


def read_metadata(
    storage_reader: Callable[ [str | dict[any,any], str], list[dict[any, any]]], 
    location: str | dict[any, any],
    md_entities: list[str] | None = None,
    exclude_entities: list[str] | None = None
) -> dict[ str, list[ dict[str,any] ] ]:
    """Read JSON metadata into a dict/list structure

    Parameters
    ----------
    storage_reader : Callable[ [str | dict[any,any], str], list[dict[any, any]]]
        A function that takes as it's arguments:
        - location : str | dict[any, any]
            Location details for the storage mechanism where metadata is stored.
        - md_entity_type : str
            The type of entity to load. Types are enumerated in 
            metadata_lib.definitions.allowed_values.ENTITIES
        The function returns: 
        metadata_entity : list[dict[any, any]]]
            A list of read metadata entities.
    location : str | dict[any, any]
        Location details for the storage mechanism where metadata is stored.
    md_entities : list[str] | None
        A list of entity types to load. If None, all entities in ENTITIES will 
        be loaded.
    
    Returns
    -------
    md_json : dict[ str, list[ dict[str,any] ] ]
        A dict/list/dict representation of all json metadata.
    
    Raises
    ------
    ValueError
        if an entity_type in md_entities is not a valid entity_type
    """
    md_json = {}
    if not md_entities:
        md_entities = ENTITIES
    for entity in md_entities:
        if entity not in ENTITIES:
            raise ValueError(f'{entity} is not a valid metadata entity.')
        if entity not in exclude_entities:
            md_json[entity] = storage_reader(location, entity)
    return md_json



def write_metadata(
    storage_writer: Callable[ [str | dict[any,any], str, list[dict[str,any]]], None], 
    location: str | dict[any, any],
    metadata_obj_or_json: dict[ str, list[any] ]
) -> None:
    """Write a metadata structure to storage, overwriting any existing files.

    Parameters
    ----------
    storage_writer : Callable[ [str | dict[any,any], str, list[dict[str,any]]], None]
        A function to write metadata to a storage location. It takes as it's 
        arguments:
        - location : str | dict[any,any]
            Location details for the storage mechanism where metadata can be 
            stored.
        - md_entity_type : str
            The type of entity to write. Types are enumerated in 
            metadata_lib.definitions.allowed_values.ENTITIES
        - entity_list : list[ dict[str,any] ]
            A list of entities to write.
    location : str | dict[any, any]
        Location details for the storage mechanism where metadata can be stored.
    metadata_obj_or_json : dict[ str, list[any] ]
        The metadata to write. This can be either in the form md_json or 
        md_objects.
    
    Notes
    -----
    This function calls FastAPI's jsonable_encoder on incoming data to make 
    sure it is json compatible. This allows it to easily process metadata in 
    the form of Pydantic objects.
    """
    # Make sure received metadata is json compatible
    md_to_write = jsonable_encoder(deepcopy(metadata_obj_or_json))
    for ent_type, ent_list in md_to_write.items():
        storage_writer(
            location,
            ent_type,
            ent_list
        )



def write_dag_config(
    storage_writer: Callable[ [str | dict[any,any], str, list[dict[str,any]]], None], 
    location: str | dict[any, any],
    dag_config: dict[ str, list[any]]
) -> None:
    """Write dag_config to storage, overwriting any existing file.

    Parameters
    ----------
    storage_writer : Callable[ [str | dict[any,any], str, list[dict[str,any]]], None]
        A function to write metadata to a storage location. It takes as it's 
        arguments:
        - location : str | dict[any,any]
            Location details for the storage mechanism where metadata can be 
            stored.
        - md_entity_type : str
            The type of entity to write. Types are enumerated in 
            metadata_lib.definitions.allowed_values.ENTITIES
        - entity_list : list[ dict[str,any] ]
            A list of entities to write.
    location : str | dict[any, any]
        Location details for the storage mechanism where metadata can be stored.
    dag_config : dict[ str, list[any]]
        The dag_config to write. 
    
    Notes
    -----
    This function calls FastAPI's jsonable_encoder on incoming data to make 
    sure it is json compatible. This allows it to easily process metadata in 
    the form of Pydantic objects.
    """
    # Make sure received metadata is json compatible
    ent_type = 'dag_config'
    config_to_write = jsonable_encoder(deepcopy(metadata_obj_or_json))
    storage_writer(
        location,
        ent_type,
        config_to_write
    )



def metadata_json_to_objects(
    metadata_json: dict[ str, list[any] ]
) -> dict[ str, list[any] ]:
    """Turn metadata entities into pydantic objects

    Parameters
    ----------
    metadata_json : dict[ str, list[any] ]
        Metadata in the form of md_json
    
    Returns
    -------
    metadata_obj : dict[ str, list[any] ]
        Metadata in the form of md_objects
    """
    # Output object
    metadata_obj = {}

    # Namespaces
    metadata_obj['namespaces'] = []
    for ns in metadata_json['namespaces']:
        metadata_obj['namespaces'].append(Namespace(**ns))
    
    # Schemas
    metadata_obj['schemas'] = []
    for sch in metadata_json['schemas']:
        metadata_obj['schemas'].append(Schema(**sch))
    
    # Systems
    metadata_obj['systems'] = []
    for sys in metadata_json['systems']:
        metadata_obj['systems'].append(System(**sys))
    
    # Data entities
    metadata_obj['data_entities'] = []
    for ent in metadata_json['data_entities']:
        metadata_obj['data_entities'].append(Data_entity(**ent))
    
    # Namespaces
    metadata_obj['pipelines'] = []
    for pl in metadata_json['pipelines']:
        metadata_obj['pipelines'].append(Pipeline(**pl))
    
    return metadata_obj



def create_id_indexes(
    metadata_obj: dict[ str, list[any] ]
) -> dict[ str, dict[int, any] ]:
    """Add indexes to metadata, allowing access by integer id

    Parameters
    ----------
    metadata_obj : dict[ str, list[any] ]
        Metadata in the form of md_objects
    
    Returns
    -------
    metadata_id_idx : dict[ str, dict[int, any] ]
        Metadata in the form of by_id
    """
    # Output object
    metadata_id_idx = {}

    # Namespaces
    metadata_id_idx['namespaces_idx'] = {}
    for ns in metadata_obj['namespaces']:
        metadata_id_idx['namespaces_idx'][ns.id] = ns
    
    # Schemas
    metadata_id_idx['schemas_idx'] = {}
    for sch in metadata_obj['schemas']:
        metadata_id_idx['schemas_idx'][sch.id] = sch
    
    # Systems
    metadata_id_idx['systems_idx'] = {}
    for sys in metadata_obj['systems']:
        metadata_id_idx['systems_idx'][sys.id] = sys
    
    # Data_entities
    metadata_id_idx['data_entities_idx'] = {}
    for ent in metadata_obj['data_entities']:
        metadata_id_idx['data_entities_idx'][ent.id] = ent
    
    # Pipelines
    metadata_id_idx['pipelines_idx'] = {}
    for pl in metadata_obj['pipelines']:
        metadata_id_idx['pipelines_idx'][pl.id] = pl
    
    return metadata_id_idx



def generate_unids(
    metadata_id_idx: dict[ str, dict[int, any] ]
) -> dict[ str, list[any] ]:
    """Take indexed and objectified metadata and generate UNIDS for each 
        metadata entities id, effectively filling the UNID field for each object
    
    Parameters
    ----------
    metadata_id_idx : dict[ str, dict[int, any] ]
        Metadata in the form of by_id
    
    Returns
    -------
    metadata_unid : dict[ str, list[any] ]
        Metadata in the form of md_objects_with_unids
    """
    # Output object
    metadata_unid = {}

    # Namespaces:
    metadata_unid['namespaces'] = []
    for ns in metadata_id_idx['namespaces_idx'].values():
        ns.unid = id_to_unid(metadata_id_idx, 'namespaces', ns.id)
        metadata_unid['namespaces'].append(ns)
    
    # Schemas:
    metadata_unid['schemas'] = []
    for sch in metadata_id_idx['schemas_idx'].values():
        sch.unid = id_to_unid(metadata_id_idx, 'schemas', sch.id)
        metadata_unid['schemas'].append(sch)
    
    # Systems:
    metadata_unid['systems'] = []
    for sys in metadata_id_idx['systems_idx'].values():
        sys.unid = id_to_unid(metadata_id_idx, 'systems', sys.id)
        metadata_unid['systems'].append(sys)
    
    # Data_entities:
    metadata_unid['data_entities'] = []
    for ent in metadata_id_idx['data_entities_idx'].values():
        ent.unid = id_to_unid(metadata_id_idx, 'data_entities', ent.id)
        metadata_unid['data_entities'].append(ent)

    
    # Pipelines:
    metadata_unid['pipelines'] = []
    for pl in metadata_id_idx['pipelines_idx'].values():
        pl.unid = id_to_unid(metadata_id_idx, 'pipelines', pl.id)
        metadata_unid['pipelines'].append(pl)
    
    return metadata_unid



def create_unid_indexes(
    metadata_unid: dict[ str, list[any] ]
) -> dict[ str, dict[str, any] ]:
    """Take indexed and objectified metadata, with UNIDs generated and 
        create UNID indexes
    
    Parameters
    ----------
    metadata_unid : dict[ str, list[any] ]
        Metadata in the form of md_objects_with_unids
    
    Returns
    -------
    metadata_unid_idx : dict[ str, dict[str, any] ]
        Metadata in the form of by_unid
    """
    # Output object
    metadata_unid_idx = {}

    # Namespaces
    metadata_unid_idx['namespaces_unid_idx'] = {}
    for ns in metadata_unid['namespaces']:
        metadata_unid_idx['namespaces_unid_idx'][ns.unid] = ns
    
    # Schemas
    metadata_unid_idx['schemas_unid_idx'] = {}
    for sch in metadata_unid['schemas']:
        metadata_unid_idx['schemas_unid_idx'][sch.unid] = sch
    
    # Systems
    metadata_unid_idx['systems_unid_idx'] = {}
    for sys in metadata_unid['systems']:
        metadata_unid_idx['systems_unid_idx'][sys.unid] = sys
    
    # Data_entities
    metadata_unid_idx['data_entities_unid_idx'] = {}
    for ent in metadata_unid['data_entities']:
        metadata_unid_idx['data_entities_unid_idx'][ent.unid] = ent
    
    # Pipelines
    metadata_unid_idx['pipelines_unid_idx'] = {}
    for pl in metadata_unid['pipelines']:
        metadata_unid_idx['pipelines_unid_idx'][pl.unid] = pl
    
    return metadata_unid_idx



def integrate_pipelines(
    metadata_id_idx: dict[ str, dict[int, any] ],
    flatten_namespaces: bool = True,
    flatten_schemas: bool = False
) -> list[Pipeline]:
    """Replace all integer references in the pipelines structure to actual 
        objects, building a complete, deep structure
    
    Parameters
    ----------
    metadata_id_idx : dict[ str, dict[int, any] ]
        Metadata in the form of by_id
    flatten_namespaces : bool (Default: True)
        If True, replace namespace id's with only the namespace name, not the 
        whole namespace object.
    flatten_schemas: bool (Default: False)
        If True, replace entity_schema id's with only the schema, not the 
        complete schema object
    
    Returns
    -------
    pipelines : list[Pipeline]
        A list of deep pipelines (all id references replaced by complete 
        objects). Also known as an integrated metadata structure.
    """
    # Output object
    pipelines = []

    # Some simpler names
    ns_byid = metadata_id_idx['namespaces_idx']
    sys_byid = metadata_id_idx['systems_idx']
    ent_byid = metadata_id_idx['data_entities_idx']
    pl_byid = metadata_id_idx['pipelines_idx']
    sch_byid = metadata_id_idx['schemas_idx']

    ## Replace all relevant integer ID's with their respective full objects
    # schemas
    for sch in metadata_id_idx['schemas_idx'].values():
        if flatten_namespaces:
            sch.namespace = ns_byid[sch.namespace].name
        else:
            sch.namespace = ns_byid[sch.namespace]
    # systems
    for sys in metadata_id_idx['systems_idx'].values():
        if flatten_namespaces:
            sys.namespace = ns_byid[sys.namespace].name
        else:
            sys.namespace = ns_byid[sys.namespace]
    # entities
    for ent in metadata_id_idx['data_entities_idx'].values():
        # Connect namespace
        if flatten_namespaces:
            ent.namespace = ns_byid[ent.namespace].name
        else:
            ent.namespace = ns_byid[ent.namespace]
        # Connect system
        ent.system = sys_byid[ent.system]
        # Connect schema
        if flatten_schemas:
            ent.entity_schema = sch_byid[ent.entity_schema].entity_schema
        else:
            ent.entity_schema = sch_byid[ent.entity_schema]
    
    # Pipelines
    for pl in metadata_id_idx['pipelines_idx'].values():
        # Set namespace
        if flatten_namespaces:
            pl.namespace = ns_byid[pl.namespace].name
        else:
            pl.namespace = ns_byid[pl.namespace]
        # Iterate over this pipelines' instances
        for i, instance in enumerate(pl.input_output):
            ## Connect entity objects to their integer id in inputs
            if isinstance(instance['input'], list):
                # List style input
                for j, inent in enumerate(instance['input']):
                    pl.input_output[i]['input'][j] = ent_byid[inent]
            else:
                # Integer input
                pl.input_output[i]['input'] = ent_byid[instance['input']]
            ## Connect entity objects to their integer id in outputs
            if isinstance(instance['output'], list):
                # List style output
                for j, outent in enumerate(instance['output']):
                    pl.input_output[i]['output'][j] = ent_byid[outent]
            else:
                # Integer style output
                pl.input_output[i]['output'] = ent_byid[instance['output']]
        pipelines.append(pl)
    
    return pipelines



def objectify_flat_instance(instance: dict[str, any]) -> SimpleNamespace:
    """Take a dict of flat instance config and transform it to a structure of 
        simplenamespaces
    """
    # Setup primary return object
    obj_instance_conf = SimpleNamespace(**instance)

    # Connect input dict "i"
    if isinstance(instance['i'], list):
        # "i" is a list, so iterate it
        in_ent_list = []
        for inent in instance['i']:
            in_ent_list.append(SimpleNamespace(**inent))
        obj_instance_conf.i = in_ent_list
    else:
        # "i" is a single instance dict, so just objectify and add it
        obj_instance_conf.i = SimpleNamespace(**instance['i'])
    
    # Connect output "o"
    if isinstance(instance['o'], list):
        # "o" is a list, so iterate it
        out_ent_list = []
        for outent in instance['o']:
            out_ent_list.append(SimpleNamespace(**outent))
        obj_instance_conf.o = out_ent_list
    else:
        # "o" is a single instance dict, so just objectify and add it
        obj_instance_conf.o = SimpleNamespace(**instance['o'])
    
    return obj_instance_conf



def flatten_instance(
    pipeline: Pipeline, 
    instance_nr: int,
    objectify_output: bool = True
) -> dict[str, any] | SimpleNamespace:
    """Take a Pydantic object of deeply integrated Pipeline and instance-nr
    Return flattened instance config dict

    Parameters
    ----------
    pipeline : Pipeline
        A deeply connected (integrated) pipeline object
    instance_nr : int
        The list index of the instance to be flattened. An instance is a 
        single input_output pair, although it can contain multiple inputs and 
        multiple outputs. A compound pipeline can have multiple instances 
        whereas a single pipeline can have only one. 
    objectify_output : bool (Default: True)
        If True, run objectify_flat_instance() on the output to produce a 
        structure of SimpleNamespaces rather than dicts, allowing easier access 
        via dot-notation.
    
    Returns
    -------
    dict[str, any] | SimpleNamespace
        A flattened pipeline instance conf, ready for consumption through a 
        pipeline
    """
    # Get a deep copy of pipeline to make assignments independent of input
    pl = pipeline.model_copy(deep=True)
    
    # Setup instance frame
    instance_conf = {}
    
    # Register pipeline-level config
    for plkey in FLAT_PL_KEYS:
        instance_conf[f'pl_{plkey}'] = getattr(pl, plkey)
    # If there is pl-level config, add it's contents
    if pl.config:
        instance_conf.update(pl.config)
    
    # Process input
    if isinstance(pl.input_output[instance_nr]['input'], list):
        # Listwise input logic
        instance_conf['i'] = [] # Multiple input entities in a list
        for ent_nr, entity in enumerate(pl.input_output[instance_nr]['input']):
            input_instance = pl.input_output[instance_nr]['input'][ent_nr]
            current_ent = {}
            for entkey in FLAT_ENT_KEYS:
                if entkey == 'entity_schema':
                    # unpack schema to dict
                    current_ent[f'ent_{entkey}'] = getattr(input_instance, entkey).model_dump()
                else:
                    current_ent[f'ent_{entkey}'] = getattr(input_instance, entkey)
            for syskey in FLAT_SYS_KEYS:
                current_ent[f'sys_{syskey}'] = getattr(input_instance.system, syskey)
            if input_instance.config:
                current_ent.update(input_instance.config)
            if input_instance.system.config:
                current_ent.update(input_instance.system.config)
            instance_conf['i'].append(current_ent)
    else:
        # Single input instance logic
        instance_conf['i'] = {} # Keys from input entities
        input_instance = pl.input_output[instance_nr]['input']
        for entkey in FLAT_ENT_KEYS:
            if entkey == 'entity_schema':
                instance_conf['i'][f'ent_{entkey}'] = getattr(input_instance, entkey).model_dump()
            else:
                instance_conf['i'][f'ent_{entkey}'] = getattr(input_instance, entkey)
        for syskey in FLAT_SYS_KEYS:
            instance_conf['i'][f'sys_{syskey}'] = getattr(input_instance.system, syskey)
        if input_instance.config:
            instance_conf['i'].update(input_instance.config)
        if input_instance.system.config:
            instance_conf['i'].update(input_instance.system.config)
    
    # Proces output
    if isinstance(pl.input_output[instance_nr]['output'], list):
        # Listwise output logic
        instance_conf['o'] = [] # Multiple output entities in a list
        for ent_nr, entity in enumerate(pl.input_output[instance_nr]['output']):
            output_instance = pl.input_output[instance_nr]['output'][ent_nr]
            current_ent = {}
            for entkey in FLAT_ENT_KEYS:
                if entkey == 'entity_schema':
                    current_ent[f'ent_{entkey}'] = getattr(output_instance, entkey).model_dump()
                else:
                    current_ent[f'ent_{entkey}'] = getattr(output_instance, entkey)
            for syskey in FLAT_SYS_KEYS:
                current_ent[f'sys_{syskey}'] = getattr(output_instance.system, syskey)
            if output_instance.config:
                current_ent.update(output_instance.config)
            if output_instance.system.config:
                current_ent.update(output_instance.system.config)
            instance_conf['o'].append(current_ent)
    else:
        # Single output instance logic
        instance_conf['o'] = {} # Keys from output entities
        output_instance = pl.input_output[instance_nr]['output']
        for entkey in FLAT_ENT_KEYS:
            if entkey == 'entity_schema':
                instance_conf['o'][f'ent_{entkey}'] = getattr(output_instance, entkey).model_dump()
            else:
                instance_conf['o'][f'ent_{entkey}'] = getattr(output_instance, entkey)
        for syskey in FLAT_SYS_KEYS:
            instance_conf['o'][f'sys_{syskey}'] = getattr(output_instance.system, syskey)
        if output_instance.config:
            instance_conf['o'].update(output_instance.config)
        if output_instance.system.config:
            instance_conf['o'].update(output_instance.system.config)
    
    if objectify_output:
        # Transform dict structure to simplenamespace objects
        return objectify_flat_instance(instance_conf)
    else:
        # Return the dict structure
        return instance_conf



def integrated_to_dag_config(
    integrated: dict[ str, list[ dict[str,any] ] ]
) -> dict[ str, list[any]]:
    """Integrated metadata and process it to config that is ready to be 
    consumed from DAGs.

    Parameters
    ----------
    integrated : dict[ str, list[ dict[str,any] ] ]
        A deeply connected (integrated) pipeline object
    
    Returns
    -------
    dict[ str, list[any]]
        Integrated conf for all pipelines, ready for consumption through a 
        DAG
    """
    enabled_pipelines = []
    for pl in integrated:
        if pl.enabled:
            enabled_pipelines.append(pl)
    conf = {}
    for pl in enabled_pipelines:
        instance_list = []
        for i, instance in enumerate(pl.input_output):
            instance_list.append(flatten_instance(
                pipeline=pl,
                instance_nr=i,
                objectify_output=False
            ))
        pipeline_identifier = pl.unid[:pl.unid.rfind('.')]
        conf[pipeline_identifier] = instance_list
    return conf



def metadata_objects_to_json(
    md_obj: dict[ str, list[any] ]
) -> dict[ str, list[any] ]:
    """Turn metadata objects into dict form

    Parameters
    ----------
    md_obj : dict[ str, list[any] ]
        Metadata in the form of md_objects
    
    Returns
    -------
    md_json : dict[ str, list[any] ]
        Metadata in the form of md_json
    """
    md_json = {} # Output object

    # Iterate over the different md entities
    for ent_type, ent_list in md_obj.items():
        # Create a list to receive the json version
        md_json[ent_type] = []
        for obj in ent_list:
            # Remove UNID if it is set
            if obj.unid:
                obj.unid = None
            # Dump the model into a dict and add to list
            md_json[ent_type].append(obj.model_dump())
    
    return md_json



def schema_unid_refs_to_ids(
    metadata_unid_idx: dict[str, dict[str, any] ],
    schema: Schema
) -> Schema:
    """Turn a schema's namespace ref from unid to id if it is a unid (str)

    Parameters
    ----------
    metadata_unid_idx : dict[str, dict[str, any] ]
        Metadata in the form of by_unid
    schema : Schema
        A schema object
    
    Return
    ------
    schema : Schema
        A schema with its refs turned to id's if they were unids.
    """
    if isinstance(schema.namespace, str):
        schema.namespace = metadata_unid_idx['namespaces_unid_idx'][schema.namespace].id
    return schema



def system_unid_refs_to_ids(
    metadata_unid_idx: dict[ str, dict[str, any] ],
    system: System
) -> System:
    """Turn a system's namespace ref from unid to id if it is a unid (str)

    Parameters
    ----------
    metadata_unid_idx : dict[str, dict[str, any] ]
        Metadata in the form of by_unid
    system : System
        A system object
    
    Return
    ------
    system : System
        A system with its refs turned to id's if they were unids.
    """
    if isinstance(system.namespace, str):
        system.namespace = metadata_unid_idx['namespaces_unid_idx'][system.namespace].id
    return system



def data_entity_unid_refs_to_ids(
    metadata_unid_idx: dict[ str, dict[str, any] ],
    data_entity: Data_entity
) -> Data_entity:
    """Turn a data_entity's reference keys from unid to id if they're unids (str)

    Parameters
    ----------
    metadata_unid_idx : dict[str, dict[str, any] ]
        Metadata in the form of by_unid
    data_entity : Data_entity
        A data_entity object
    
    Return
    ------
    data_entity : Data_entity
        A data_entity with its refs turned to id's if they were unids.
    """
    # Namespace
    if isinstance(data_entity.namespace, str):
        data_entity.namespace = metadata_unid_idx['namespaces_unid_idx'][data_entity.namespace].id
    # System
    if isinstance(data_entity.system, str):
        data_entity.system = metadata_unid_idx['systems_unid_idx'][data_entity.system].id
    # Schema
    if isinstance(data_entity.entity_schema, str):
        data_entity.entity_schema = metadata_unid_idx['schemas_unid_idx'][data_entity.entity_schema].id
    return data_entity



def pipeline_unid_refs_to_ids(
    metadata_unid_idx: dict[ str, dict[str, any] ],
    pipeline: Pipeline
) -> Pipeline:
    """Turn a pipeline's reference keys from unid to id if they're unids (str)

    Parameters
    ----------
    metadata_unid_idx : dict[str, dict[str, any] ]
        Metadata in the form of by_unid
    pipeline : Pipeline
        A pipeline object
    
    Return
    ------
    pipeline : Pipeline
        A pipeline with its refs turned to id's if they were unids.
    """
    # Namespace
    if isinstance(pipeline.namespace, str):
        pipeline.namespace = metadata_unid_idx['namespaces_unid_idx'][pipeline.namespace].id
    # input_output
    for i, instance in enumerate(pipeline.input_output):
        if isinstance(instance['input'], list):
            # Multiple input entities. Iterate
            for j, inent in enumerate(instance['input']):
                if isinstance(inent, str):
                    pipeline.input_output[i]['input'][j] = metadata_unid_idx['data_entities_unid_idx'][inent].id
        else:
            # Single input entity
            if isinstance(instance['input'], str):
                instance['input'] = metadata_unid_idx['data_entities_unid_idx'][instance['input']].id
        if isinstance(instance['output'], list):
            # Multiple output entities. Iterate
            for j, outent in enumerate(instance['output']):
                if isinstance(outent, str):
                    pipeline.input_output[i]['output'][j] = metadata_unid_idx['data_entities_unid_idx'][outent].id
        else:
            # Single output entity
            if isinstance(instance['output'], str):
                instance['output'] = metadata_unid_idx['data_entities_unid_idx'][instance['output']].id
    return pipeline



def unid_refs_to_ids(
    metadata_unid_idx: dict[ str, dict[str, any] ],
    obj: Schema | System | Data_entity | Pipeline
) -> Schema | System | Data_entity | Pipeline:
    """Turn all foreign keys of the object from UNIDs into id's

    Parameters
    ----------
    metadata_unid_idx : dict[ str, dict[str, any] ]
        Metadata in the form of by_unid
    obj : Schema | System | Data_entity | Pipeline
        A metadata entity object
    
    Returns
    -------
    obj : Schema | System | Data_entity | Pipeline
        A metadata entity object
    """
    ## Select function based on obj type
    if isinstance(obj, Schema):
        obj = schema_unid_refs_to_ids(metadata_unid_idx, obj)
    elif isinstance(obj, System):
        obj = system_unid_refs_to_ids(metadata_unid_idx, obj)
    elif isinstance(obj, Data_entity):
        obj = data_entity_unid_refs_to_ids(metadata_unid_idx, obj)
    elif isinstance(obj, Pipeline):
        obj = pipeline_unid_refs_to_ids(metadata_unid_idx, obj)
    return obj # Namespace falls through unchanged



def objects_from_unid_index(
    metadata_unid_idx: dict[ str, dict[str, any] ]
) -> dict[ str, list[any] ]:
    """Turn a unid index back to objects

    Parameters
    ----------
    metadata_unid_idx : dict[ str, dict[str, any] ]
        Metadata in the form of by_unid
    
    Returns
    -------
    md_objects : dict[ str, list[any] ]
        Metadata in the form of md_objects
    """
    md_objects = {
        'namespaces': [],
        'schemas': [],
        'systems': [],
        'data_entities': [],
        'pipelines': []
    }
    for ent_type, ent_list in md_objects.items():
        for unid, obj in metadata_unid_idx[f'{ent_type}_unid_idx'].items():
            ent_list.append(obj)
    
    return md_objects



def objects_from_id_index(
    metadata_id_idx: dict[ str, dict[str, any] ]
) -> dict[ str, list[any] ]:
    """Turn an id index back to objects

    Parameters
    ----------
    metadata_id_idx : dict[ str, dict[str, any] ]
        Metadata in the form of by_id
    
    Returns
    -------
    md_objects : dict[ str, list[any] ]
        Metadata in the form of md_objects
    """
    md_objects = {
        'namespaces': [],
        'schemas': [],
        'systems': [],
        'data_entities': [],
        'pipelines': []
    }
    for ent_type, ent_list in md_objects.items():
        for id, obj in metadata_id_idx[f'{ent_type}_idx'].items():
            ent_list.append(obj)
    
    return md_objects