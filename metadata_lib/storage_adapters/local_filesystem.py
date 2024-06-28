"""Read metadata from a local Linux filesystem
"""
import json
from pathlib import Path
from .. definitions import ENTITIES
from . import StorageAdapter

from typing import Union, Dict, List, Any


class LocalFilesystem(StorageAdapter):
    """Read and write to local folders
    """
    def exists(
        self,
        location: str
    ) -> bool:
        """Check if the local filder in location exists

        Parameters
        ----------
        location : str
            The information needed to address the location. We expect a str 
            that is a valid path on the local filesystem.
        
        Returns
        -------
        bool
            True if the location exists. False if it does not exist.
        
        Raises
        ------
        TypeError
            If location is not of type str.
        """
        if not isinstance(location, str):
            # In localstorage, we only expect a string with the directory
            msg = f'Localfilesystem expects location as a string, not {type(location).__name__}.'
            raise TypeError(msg)
        return Path(location).resolve().exists()



    def create(
        self,
        location: str
    ) -> None:
        """Create the storage location if it does not exist

        Parameters
        ----------
        location : str
            The information needed to address the location. We expect a str 
            that is a valid path on the local filesystem.
        
        Raises
        ------
        TypeError
            If location is not of type str.
        """
        if not isinstance(location, str):
            # In localstorage, we only expect a string with the directory
            msg = f'Localfilesystem expects location as a string, not {type(location).__name__}.'
            raise TypeError(msg)
        # Create the directory if it does not exist
        Path(location).resolve().mkdir(parents=True, exist_ok=True)



    def read(
        self,
        location: str,
        md_entity_type: str
    ) -> list[ dict[any,any] ]:
        """Read a single metadata entity file from local filesystem into a 
            list of dicts.
        
        Parameters
        ----------
        location : str
            The information needed to address the location. We expect a str 
            that is a valid path on the local filesystem.
        md_entity_type : str
            The entity type to read and load. Should be a valid entity_type 
            according to metadata_lib.definitions.allowed_values.ENTITIES.
        
        Returns
        -------
        list[ dict[any,any] ]
            A list of dicts where each dict is an entity from the specified 
            type.
        
        Raises
        ------
        TypeError
            If location is not of type str.
        ValueError
            If md_entity_type is not in 
            metadata_lib.definitions.allowed_values.ENTITIES
        """
        if not isinstance(location, str):
            # In localstorage, we only expect a string with the directory
            msg = f'Localfilesystem expects location as a string, not {type(location).__name__}.'
            raise TypeError(msg)
        if md_entity_type not in ENTITIES:
            raise ValueError(f'{md_entity_type} is not a valid metadata entity.')
        md_file = Path(location).joinpath(f'{md_entity_type}.json').resolve()
        with open(md_file, 'r') as f:
            return json.load(f)
    

    def write(
        self,
        location: str,
        md_entity_type: str,
        entity_list: list[ dict[str,any] ]
    ) -> None:
        """Write a single metadata entity file to local filesystem.

        Parameters
        ----------
        location : str
            The information needed to address the location. We expect a str 
            that is a valid path on the local filesystem.
        md_entity_type : str
            The entity type to read and load. Should be a valid entity_type 
            according to metadata_lib.definitions.allowed_values.ENTITIES.
        entity_list : list[ dict[str,any] ]
            A list of dicts where each dict is an entity of the specified type.
            The dicts should be json compatible. We suggest running 
            entities through FastAPI's json_compatible_encoder before passing 
            them to a storage adapter for writing.
        
        Raises
        ------
        TypeError
            If location is not of type str.
        ValueError
            If md_entity_type is not in 
            metadata_lib.definitions.allowed_values.ENTITIES
        """
        # Checks
        if not isinstance(location, str):
            # In localstorage, we only expect a string with the directory
            msg = f'Localfilesystem expects location as a string, not {type(location).__name__}.'
            raise TypeError(msg)
        if md_entity_type not in ENTITIES:
            raise ValueError(f'{md_entity_type} is not a valid metadata entity.')
        # Create full path to file
        md_file = Path(location).joinpath(f'{md_entity_type}.json').resolve()
        with open(md_file, 'w') as f:
            json.dump(entity_list, f, indent=4)