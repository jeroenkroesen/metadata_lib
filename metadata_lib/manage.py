"""The main entrypoint for running the metadata manager from a 
    Jupyter Lab notebook.

Notes
-----
Het doel van de metadata manager is het beheren van Dataplatform Metadata door 
data specialisten. Metadata bestaat momenteel uit een verzameling json files. 
Hier is voor gekozen, omdat het beheren en hosten van een SQL database niet 
in verhouding stond tot het volume van de data en het eenvoud van de 
data structuur.
"""

from copy import deepcopy
import pendulum
from types import SimpleNamespace
import pandas as pd
from rich import print
from . schemas import (Namespace, Schema, System, Data_entity, Pipeline)
from . build import (
    read_metadata, write_metadata, metadata_json_to_objects, create_id_indexes, 
    generate_unids, create_unid_indexes, integrate_pipelines, 
    metadata_objects_to_json, unid_refs_to_ids, objects_from_unid_index, 
    objects_from_id_index, integrated_to_dag_config
)
from . view import (
    all_ids_to_unids, metadata_obj_to_records, metadata_records_to_dataframes
)
from . validate import (
    validate_schema, validate_system, validate_data_entity, validate_pipeline,
    validate_new_namespace, validate_new_schema, validate_new_system,
    validate_new_data_entity, validate_new_pipeline, get_entity_dependants
)
from . definitions import ENTITIES

# Typing imports
from . storage_adapters import StorageAdapter




class MetadataLocation():
    """Representation of storage for metadata json files

    Provide information about the available storage location and 
    an interface to read and write from and to the location.

    Attributes
    ----------
    storage_type : str
        The type of storage this location uses. Examples: localstorage,
        google_cloud_storage etc.
    storage_adapter : StorageAdapter
        An instance of StorageAdapter. StorageAdapters implementations of 
        abstract base class StorageAdapter and implement methods to read from 
        and write to the specified storage type. Check See Also for more 
        information on StorageAdapters.
    address : str | dict[any,any]
        Information necessary for the storage_adapter to access the storage
        location. For example: in localstorage this can be a string with the 
        path to a local file. In addressing blob storage etc, something more 
        flexibel might be necessary (bucket + folder). This flexibility is 
        provided by allowing address to be a dict with any kinds of keys and 
        values.
    entities_to_load : list[str] | None (default: None)
        An optional list of entity_types to load from this storage. If emtpy, 
        manager will attempt to load all entity_types.
    is_git_repo : bool (default: True)
        Indicate if the location is also a git repository. Currently not 
        used and reserved for future implementation of git related 
        funtionality.
    
    See Also
    --------
    metadata_lib.storage_adapters.abstract_adapter.py
    metadata_lib.storage_adapters.local_filesystem.py
    """
    storage_type: str
    storage_adapter: StorageAdapter
    address: str | dict[any,any]
    entities_to_load: list[str] | None = None
    is_git_repo: bool = True

    def __init__(
        self,
        storage_type: str,
        Storage_Adapter: type[StorageAdapter],
        address: str | dict[any,any],
        entities_to_load: list[str] | None = None,
        is_git_repo: bool = True
    ) -> None:
        self.storage_type = storage_type
        self.storage_adapter = Storage_Adapter()
        self.address = address
        self.entities_to_load = entities_to_load
        self.is_git_repo = is_git_repo




class MetadataStructure():
    """A metadata structure with various representations of the metadata, 
        such as indexes by id and by unid.
    
    Attributes
    ----------
    md_json : dict[ str,list[any] (Default: None)
        Metadata read from json and stored in memory as a dict with lists of 
        dicts. Keys will be entity_types as listed in allowed_values.
    md_objects : dict[ str,list[Namespace | Schema | System | Data_entity | Pipeline]] (Default: None)
        The same overall structure as md_json, but the dicts containing the 
        actual entities have been read into their corresponding Pydantic 
        objects for validation and dot access.
    md_objects_with_unids : dict[ str,list[Namespace | Schema | System | Data_entity | Pipeline]] (Default: None)
        The same as md_objects, but the "unid" key of each object is now 
        filled in.
    by_id : dict[ str,dict[int, Namespace | Schema | System | Data_entity | Pipeline]] (Default: None)
        A structure of Pydantic objects whereby an object is accessible in the 
        dict of objects by it's id.
    by_unid : dict[ str,dict[str, Namespace | Schema | System | Data_entity | Pipeline]] Default: None)
        The same as by_id, only here the objects are accessible by their unid.
    integrated : integrated: dict[ str, list[ dict[str,any] ] ] (Default: None)
        A deep structure where id references to other objects are replaced 
        with that full object.
    dag_config : dict[ str, list[any]]
        Config of all enabled pipelines that is ready to be consumed by a DAG
    view : A dict of entity_types as keys and dataframes as values. This is 
        optimized for human reading of metadata.
    valid : bool (Default: False)
        Indicator if the loaded structure is a valid structure.
    report : dict[str,any] (Default: None)
        The report of the most recent validation. Inspect this to find errors 
        after failed validation.
    
    See Also
    --------
    metadata_lib.definitions.allowed_values.py
    metadata_lib.schemas
    """
    md_json: dict[ str,list[any] ] = None
    md_objects: dict[ str,list[Namespace | Schema | System | Data_entity | Pipeline]] = None
    md_obj_with_unids: dict[ str,list[Namespace | Schema | System | Data_entity | Pipeline]] = None
    by_id: dict[ str,dict[int, Namespace | Schema | System | Data_entity | Pipeline]] = None
    by_unid: dict[ str,dict[str, Namespace | Schema | System | Data_entity | Pipeline]] = None
    integrated: dict[ str, list[ dict[str,any] ] ] = None
    dag_config: dict[ str, list[any]] = None
    view: dict[str,pd.DataFrame] = None
    valid: bool = False
    report: dict[str,any] = None


    def __init__(self, md_json: dict[ str,list[any] ]) -> None:
        self.md_json = deepcopy(md_json)
        self.build_from_json()
    

    def build_from_json(self) -> None:
        """(Re)build all metadata structures from md_json
        """
        self.md_objects = metadata_json_to_objects(deepcopy(self.md_json))
        self.by_id = create_id_indexes(deepcopy(self.md_objects))
        self.md_obj_with_unids = generate_unids(self.by_id)
        self.by_unid = create_unid_indexes(self.md_obj_with_unids)
        self.integrated = integrate_pipelines(deepcopy(self.by_id))
        self.dag_config = integrated_to_dag_config(deepcopy(self.integrated))
        unidseverywhere = all_ids_to_unids(deepcopy(self.by_id))
        records = metadata_obj_to_records(unidseverywhere)
        self.view = metadata_records_to_dataframes(records)
        self.validate()
    

    def build_from_objects(self) -> None:
        """(Re)build all metadata structures from md_objects (including 
            md_json)
        """
        self.md_json = metadata_objects_to_json(deepcopy(self.md_objects))
        self.by_id = create_id_indexes(deepcopy(self.md_objects))
        self.md_obj_with_unids = generate_unids(self.by_id)
        self.by_unid = create_unid_indexes(self.md_obj_with_unids)
        self.integrated = integrate_pipelines(deepcopy(self.by_id))
        unidseverywhere = all_ids_to_unids(deepcopy(self.by_id))
        records = metadata_obj_to_records(unidseverywhere)
        self.view = metadata_records_to_dataframes(records)
        self.validate()
    

    def validate(self) -> dict[str,any]:
        """Validate the entire metadata structure

        Returns
        -------
        report : dict[str,any]
            The report with results of validation. The following keys will be 
            present in the report:
            report["valid"] : bool
                True if validation passed, False if it failed
            report["last_validated"] : datetime
                The UTC datetime of the last run validation.
            Keys with the names of entity_types, 
            for example: report["schemas"] : list[any]
                If validation passes, all such lists will be empty. If 
                validation fails, reasons for the fail will be in these lists.
        """
        # Setup report
        report = {
            'valid': True,
            'last_validated': pendulum.now(tz='UTC'),
            'schemas': [],
            'systems': [],
            'data_entities': [],
            'pipelines': []
        }
        # Validate schemas
        for schema in self.md_objects['schemas']:
            result = validate_schema(schema, self.by_id, self.by_unid)
            if not result['valid']:
                report['valid'] = False
            report['schemas'].append(result)
        # Validate systems
        for system in self.md_objects['systems']:
            result = validate_system(
                system=system,
                metadata_obj=self.md_objects,
                metadata_id_idx=self.by_id, 
                metadata_unid_idx=self.by_unid
            )
            if not result['valid']:
                report['valid'] = False
            report['systems'].append(result)
        # Validate data_entities
        for data_entity in self.md_objects['data_entities']:
            result = validate_data_entity(
                data_entity=data_entity,
                metadata_obj=self.md_objects,
                metadata_id_idx=self.by_id, 
                metadata_unid_idx=self.by_unid
            )
            if not result['valid']:
                report['valid'] = False
            report['data_entities'].append(result)
        # Validate pipelines
        for pipeline in self.md_objects['pipelines']:
            result = validate_pipeline(
                pipeline=pipeline,
                metadata_obj=self.md_objects,
                metadata_id_idx=self.by_id, 
                metadata_unid_idx=self.by_unid
            )
            if not result['valid']:
                report['valid'] = False
            report['pipelines'].append(result)
        
        # Process results
        if report['valid']:
            self.valid = True
        else:
            self.valid = False
        self.report = report
        return report




class MetadataManager():
    """Perform management on DataPlatform metadata

    MetadataManager (MM) is ment to be run from Jupyter Lab by Data Specialists 
    to manage the DataPlatform by mutating it's metadata. A template notebook 
    is available in this repo for specialists to use.

    MM uses two stores on disk:
    - store is the primary storage location and is ment to be a git repository 
        of DataPlatform Metadata. This is "the official storage". Updates to 
        it are ment to be pushed to github for further processing (testing) 
        and deployment (main)
    - stash is ment as an intermediate storage location where you can put 
        temporary changes to metadata while you are working. 
    
    In addition to disk storage, MM has 3 memory structures available to the 
    specialist:
    - current is an in-memory data structure of the data currently in the store.
        It is NOT MENT TO BE MUTATED. current serves as a reference to what 
        metadata is currently in store.
    - workspace is an in-memory store as well. This is where work is done by 
        the specialists. When loading the MM, you can choose whether to load 
        workspace from the stash or from the store.
    - cache is normally empty and is only used by MM itself when executing 
        operations. For example: it is used as a temporary structure 
        for validation purposes.

    Attributes
    ----------
    store : MetadataLocation
        Information about the storage location of store.
    stash : MetadataLocation
        Information about the storage location of stash.
    current : MetadataStructure
        A pristine structure representing what is currently stored in store.
    workspace : MetadataStructure
        Memory structure for the data specialist to operate on to mutate 
        metadata.
    cache : MetadataStructure
        Memory structure reserverd for the MM itself to be used during 
        mutation executions.
    """
    store: MetadataLocation
    stash: MetadataLocation
    current: MetadataStructure | None = None
    workspace: MetadataStructure | None = None
    cache: MetadataStructure | None = None

    def __init__(
        self,
        store: MetadataLocation,
        stash: MetadataLocation,
        load_current: bool = True,
        load_workspace: bool = False # 'stash' | 'store'
    ) -> None:
        self.store = store
        self.stash = stash
        if load_current: self.load_current()
        if load_workspace:
            self.load_workspace(load_workspace)
    

    def load_current(self) -> None:
        """Load the 'current' structure from 'store' location
        """
        self.current = MetadataStructure(
            read_metadata(
                storage_reader=self.store.storage_adapter.read,
                location=self.store.address
        ))
    

    def load_workspace(self, source: str) -> None:
        """Load workspace structure from designated source

        Parameters
        ----------
        source : str
            The location to load from. Valid options are:
            - store
            - stash
        
        Raises
        ------
        ValueError
            If the passed source is neither store nor stash
        """

        if source == 'store':
            self.workspace = MetadataStructure(
                read_metadata(
                    storage_reader=self.store.storage_adapter.read,
                    location=self.store.address
            ))
        elif source == 'stash':
            self.workspace = MetadataStructure(
                read_metadata(
                    storage_reader=self.stash.storage_adapter.read,
                    location=self.stash.address
            ))
        else:
            raise ValueError(f'Source must be store or stash. Not {source}')
    

    def create_stash(self) -> None:
        """Create a stash based on information in stash object
        """
        self.stash.storage_adapter.create(self.stash.address)
    

    def stash_workspace(self) -> bool:
        """Write workspace to stash

        Returns
        -------
        bool
            True if write is succesfull. False if user aborted.
        """
        if not self.workspace.valid:
            print('WARNING! Workspace is in an invalid state.')
            go_on = input('Continue (y), or abort (n)?')
            while go_on.lower() != 'y' and go_on.lower() != 'n':
                print(f'{go_on} is not valid input.')
                go_on = input('Continue (y), or abort (n)?')
            if go_on == 'n':
                print('Aborting save to stash')
                return False
        # Perform write to stash
        print('Writing all metadata in Workspace to Stash.')
        write_metadata(
            storage_writer=self.stash.storage_adapter.write, 
            location=self.stash.address,
            metadata_obj_or_json=self.workspace.md_objects
        )
        print('Write complete')
        return True
    

    def store_workspace(self) -> bool:
        """Write workspace to store, but only if it is valid

        Returns
        -------
        bool
            True if write is successfull, False if user cancelled or 
            write failed because structure is invalid.
        """
        if not self.workspace.valid:
            print('ERROR!! Workspace is in an invalid state.')
            print('Inspect workspace.report to find out what is going on')
            print('Consider writing to stash instead of store.')
            print('Stash will accept an invalid workspace with a warning.')
            return False
        # Performing write to store
        print('Workspace is a valid structure. Writing to Store.')
        write_metadata(
            storage_writer=self.store.storage_adapter.write, 
            location=self.store.address,
            metadata_obj_or_json=self.workspace.md_objects
        )
        print('Write complete')
        # Reload current from store
        print('Reloading current from store')
        self.load_current
        return True
    

    def add_new_entity(
        self,
        entity: Namespace | Schema | System | Data_entity | Pipeline
    ) -> dict[str,any] | bool:
        """Validate new entity against workspace. If it passes, add it to 
            workspace
        
        Parameters
        ----------
        entity : Namespace | Schema | System | Data_entity | Pipeline
            The metadata entity to validate
        
        Returns
        -------
        dict[str,any] | None
            If there is a validation error, return the validation report.
            If user cancels the operation: return False
            If operation succeedes to the end: return True
        
        Raises
        ------
        TypeError
            If entity is of a type this method cannot process.
        """
        # Choose validator for object type and run validation
        if isinstance(entity, Namespace):
            result = validate_new_namespace(
                namespace=entity,
                metadata_id_idx=self.workspace.by_id,
                metadata_unid_idx=self.workspace.by_unid
            )
        elif isinstance(entity, Schema):
            result = validate_new_schema(
                schema=entity,
                metadata_id_idx=self.workspace.by_id,
                metadata_unid_idx=self.workspace.by_unid
            )
        elif isinstance(entity, System):
            result = validate_new_system(
                system=entity,
                metadata_obj=self.workspace.md_objects,
                metadata_id_idx=self.workspace.by_id,
                metadata_unid_idx=self.workspace.by_unid
            )
        elif isinstance(entity, Data_entity):
            result = validate_new_data_entity(
                data_entity=entity,
                metadata_obj=self.workspace.md_objects,
                metadata_id_idx=self.workspace.by_id,
                metadata_unid_idx=self.workspace.by_unid
            )
        elif isinstance(entity, Pipeline):
            result = validate_new_pipeline(
                pipeline=entity,
                metadata_obj=self.workspace.md_objects,
                metadata_id_idx=self.workspace.by_id,
                metadata_unid_idx=self.workspace.by_unid
            )
        else:
            ent_type = type(entity).__name__
            raise TypeError(f'Cannot process entity of type {ent_type}')
        # Process results in case of invalidity
        if not result['valid']:
            # Inform user we can't add an invalid object and why
            print(f'ERROR! Adding this object leads to invalid metadata.')
            print()
            print('The following errors were found in validation:')
            print()
            for error in result['errors']:
                print(error)
                print()
            return result
        # Results are valid. Offer user choice to add to workspace
        print('Your new metadata entity is valid.')
        print()
        choice = input('Do you want to add your entity to Workspace? (y/n): ')
        while choice.lower() != 'y' and choice.lower() != 'n':
            print()
            print(f'{choice} is not a valid option. Enter y or n.')
            print()
            choice = input('Do you want to add your entity to Workspace? (y/n): ')
        if choice.lower() == 'n':
            print('Aborting')
            return False
        if choice.lower() == 'y':
            ## Take steps to add object to Workspace
            entity = unid_refs_to_ids(
                metadata_unid_idx=self.workspace.by_unid,
                obj=entity
            )
            # Fill cache from Workspace
            self.cache = deepcopy(self.workspace)
            # Add object to cache structure and sort list of objects by id
            if isinstance(entity, Namespace):
                self.cache.md_objects['namespaces'].append(entity)
                self.cache.md_objects['namespaces'].sort(key=lambda x: x.id)
            elif isinstance(entity, Schema):
                self.cache.md_objects['schemas'].append(entity)
                self.cache.md_objects['schemas'].sort(key=lambda x: x.id)
            elif isinstance(entity, System):
                self.cache.md_objects['systems'].append(entity)
                self.cache.md_objects['systems'].sort(key=lambda x: x.id)
            elif isinstance(entity, Data_entity):
                self.cache.md_objects['data_entities'].append(entity)
                self.cache.md_objects['data_entities'].sort(key=lambda x: x.id)
            elif isinstance(entity, Pipeline):
                self.cache.md_objects['pipelines'].append(entity)
                self.cache.md_objects['pipelines'].sort(key=lambda x: x.id)
            # Rebuild cache from its object structure, which also validates it.
            self.cache.build_from_objects()
            # Check if cache is still a valid structure
            if self.cache.valid:
                print('Your new entity is added to cache.')
                print('Cache is still a valid structure')
                print('Copying cache to Workspace and deleting cache')
                self.workspace = deepcopy(self.cache)
                self.cache = None
                print("If you're happy with the state of workspace...")
                print("don't forget to save to storage by calling: ")
                print('mm.stash_workspace()')
                print('or mm.store_workspace()')
                return True
            else:
                print('Adding your new object made cache an invalid structure.')
                print('Aborting')
                print('Inspect mm.cache.report to find out what is wrong.')
                return False
    

    def get_entity_by_unid(
        self, entity_type: str, unid: str
    ) -> Namespace | Schema | System | Data_entity | Pipeline:
        """Return a deep copy of the requested entity so it can be edited
            and passed to update_entity() for updating.
        
        Parameters
        ----------
        entity_type : str
            The type of the requested entity. Accepted types are described in 
            metadata_lib.definitions.allowed_values.ENTITIES
        unid : str
            The unid of the entity we want a copy of

        Returns
        -------
        Namespace | Schema | System | Data_entity | Pipeline
            A deepcopy of the requested entity
        
        Raises
        ------
        ValueError
            - In case of an unknown entity type
            - In case of a unid that does not exist
        """
        if not entity_type in ENTITIES:
            raise ValueError(
                f'{entity_type} is not a valid entity type.'
            )
        if not unid in self.workspace.by_unid[f'{entity_type}_unid_idx'].keys():
            raise ValueError(
                f'There are no {entity_type} with unid {unid}'
            )
        return deepcopy(
            self.workspace.by_unid[f'{entity_type}_unid_idx'][unid]
        )


    def update_entity(
        self, entity: Namespace | Schema | System | Data_entity | Pipeline
    ) -> None:
        """Update an existing entity after validating it

        Parameters
        ----------
        entity : Namespace | Schema | System | Data_entity | Pipeline
            The entity we want to update.
        
        Raises
        ------
        ValueError
            The entity we want to update does not exist.
        TypeError
            Entity is of a type this method can not process.
        """
        ## Create a cache structure where the new entity and structure can be
        ## validated.
        self.cache = deepcopy(self.workspace)
        # Strip incoming entity of it's unid references
        entity = unid_refs_to_ids(
                metadata_unid_idx=self.workspace.by_unid,
                obj=deepcopy(entity)
            )
        # Update modified
        entity.modified = pendulum.now(tz='Europe/Amsterdam')
        # Unset entity's unid in case it's set
        entity.unid = None
        if isinstance(entity, Namespace):
            # Make sure we're working on an existing entity
            if not entity.id in self.cache.by_id['namespaces_idx'].keys():
                raise ValueError(
                    f'Namespace with id {entity.id} not found in indexes'
                )
            ## Update entity and rebuild cache, which validates automatically
            # Update entity in index
            self.cache.by_id['namespaces_idx'][entity.id] = entity
        elif isinstance(entity, Schema):
            # Make sure we're working on an existing entity
            if not entity.id in self.cache.by_id['schemas_idx'].keys():
                raise ValueError(
                    f'Schema with id {entity.id} not found in indexes'
                )
            ## Update entity and rebuild cache, which validates automatically
            # Update entity in index
            self.cache.by_id['schemas_idx'][entity.id] = entity
        elif isinstance(entity, System):
            # Make sure we're working on an existing entity
            if not entity.id in self.cache.by_id['systems_idx'].keys():
                raise ValueError(
                    f'System with id {entity.id} not found in indexes'
                )
            ## Update entity and rebuild cache, which validates automatically
            # Update entity in index
            self.cache.by_id['systems_idx'][entity.id] = entity
        elif isinstance(entity, Data_entity):
            # Make sure we're working on an existing entity
            if not entity.id in self.cache.by_id['data_entities_idx'].keys():
                raise ValueError(
                    f'Data_entity with id {entity.id} not found in indexes'
                )
            ## Update entity and rebuild cache, which validates automatically
            # Update entity in index
            self.cache.by_id['data_entities_idx'][entity.id] = entity
        elif isinstance(entity, Pipeline):
            # Make sure we're working on an existing entity
            if not entity.id in self.cache.by_id['pipelines_idx'].keys():
                raise ValueError(
                    f'Pipeline with id {entity.id} not found in indexes'
                )
            ## Update entity and rebuild cache, which validates automatically
            # Update entity in index
            self.cache.by_id['pipelines_idx'][entity.id] = entity
        else:
            raise TypeError(
                f'update_entity() cannot process objects of type {type(entity).__name__}'
            )
        # Recreate md_objects from index
        self.cache.md_objects = deepcopy(objects_from_id_index(
            metadata_id_idx=self.cache.by_id
        ))
        # Rebuild cache from objects, which validates it
        self.cache.build_from_objects()
        # Copy cache to workspace if it is still valid
        if self.cache.valid:
            print('Your update was applied to cache.')
            print('Cache is still a valid structure')
            print('Copying cache to Workspace and deleting cache')
            self.workspace = deepcopy(self.cache)
            self.cache = None
            print("If you're happy with the state of workspace...")
            print("don't forget to save to storage by calling: ")
            print('mm.stash_workspace()')
            print('or mm.store_workspace()')
        else:
            print('Your update made cache an invalid structure.')
            print('Aborting')
            print('Inspect mm.cache.report to find out what is wrong.')
    

    def delete_entity(
        self, entity_type: str, unid: str
    ) -> bool | dict[ str, bool | list[any] ]:
        """Delete an entity from workspace structure

        Parameters
        ----------
        entity_type : str
            The type of the entity we want to delete. Types can be found in 
            metadata_lib.definition.allowed_values.ENTITIES
        unid : str
            The unid of the entity we want to delete.
        
        Returns
        -------
        bool | dict[ str, bool | list[any] ]
            - Return dependency report if there are other entities in the 
                structure that depend on this one. The entity will not be 
                deleted as long as it has dependants.
            - Return True if the entity was deleted
            - Return False if deleting the entity would create an invalid 
                structure.
        """
        if not entity_type in ENTITIES:
            raise ValueError(
                f'{entity_type} is not a valid entity type.'
            )
        if not unid in self.workspace.by_unid[f'{entity_type}_unid_idx'].keys():
            raise ValueError(
                f'There are no {entity_type} with unid {unid}'
            )
        # Check if entity is depended upon by others
        entity = self.get_entity_by_unid(
            entity_type=entity_type,
            unid=unid
        )
        if entity_type != 'pipelines':
            dep_report = get_entity_dependants(
                entity=entity,
                metadata_obj=self.workspace.md_objects
            )
            if dep_report['has_dependants']:
                print("The object you're trying to delete is depended upon by other objects.")
                print("Aborting")
                print("Please inspect the dependants report returned by delete_entity()")
                return dep_report
        ## Create a cache structure where the new entity and structure can be
        ## validated.
        self.cache = deepcopy(self.workspace)
        ## Delete the specified entity from cache
        # Delete from unid index first
        del self.cache.by_unid[f'{entity_type}_unid_idx'][unid]
        # Get objects from unid_idx
        self.cache.md_objects = objects_from_unid_index(
            deepcopy(self.cache.by_unid)
        )
        # Rebuild cache from objects, which validates it
        self.cache.build_from_objects()
        # Copy cache to workspace if it is still valid
        if self.cache.valid:
            print('Your delete operation was applied to cache.')
            print('Cache is still a valid structure')
            print('Copying cache to Workspace and deleting cache')
            self.workspace = deepcopy(self.cache)
            self.cache = None
            print("If you're happy with the state of workspace...")
            print("don't forget to save to storage by calling: ")
            print('mm.stash_workspace()')
            print('or mm.store_workspace()')
            return True
        else:
            print('Your update made cache an invalid structure.')
            print('Aborting')
            print('Inspect mm.cache.report to find out what is wrong.')
            return False