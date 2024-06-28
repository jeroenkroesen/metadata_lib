"""Provide an Abstract Base Class for storage adapters
"""
from abc import ABC, abstractmethod


class StorageAdapter(ABC):
    """Abstract base class for adapter type hinting

    This base class provides a common interface that all storage adapters must 
    implement to be usable. A storage adapter class must inherit from this 
    abstract base class to be accepted my the metadata manager.
    """

    @abstractmethod
    def exists(
        self,
        location: str | dict[any,any]
    ) -> bool:
        """Check if the specified location exists

        Parameters
        ----------
        location : str | dict[any,any]
            The information needed to address the location. Whether a str or 
            dict is used can ben choosen by the implementer. Location should 
            contain any and all information necessary for the storage_adapter 
            to access the storage.
        
        Returns
        -------
        bool
            True if the location exists. False if it does not exist.
        
        Raises
        ------
        TypeError
            Implementations are expected to raise a TypeError with descriptive 
            error message to indicate that they received location in an 
            unexpected format (ie received a str, expected a dict).
        """
        pass


    @abstractmethod
    def create(
        self,
        location: str | dict[any,any]
    ) -> None:
        """Create the specified storage location

        Parameters
        ----------
        location : str | dict[any,any]
            The information needed to address the location. Whether a str or 
            dict is used can ben choosen by the implementer. Location should 
            contain any and all information necessary for the storage_adapter 
            to access the storage.
        
        Raises
        ------
        TypeError
            Implementations are expected to raise a TypeError with descriptive 
            error message to indicate that they received location in an 
            unexpected format (ie received a str, expected a dict).
        """
        pass


    @abstractmethod
    def read(
        self,
        location: str | dict[any,any],
        md_entity_type: str
    ) -> list[ dict[any,any] ]:
        """Read the single json file of specified entity type to list of dicts.

        Parameters
        ----------
        location : str | dict[any,any]
            The information needed to address the location. Whether a str or 
            dict is used can ben choosen by the implementer. Location should 
            contain any and all information necessary for the storage_adapter 
            to access the storage.
        md_entity_type : str
            The entity type to read and load. Implementations are expected to 
            check that the entity_type is a valid entity_type according to 
            metadata_lib.definitions.allowed_values.ENTITIES.
        
        Returns
        -------
        list[ dict[any,any] ]
            A list of dicts where each dict is an entity from the specified 
            type.
        
        Raises
        ------
        TypeError
            Implementations are expected to raise a TypeError with descriptive 
            error message to indicate that they received location in an 
            unexpected format (ie received a str, expected a dict).
        ValueError
            Implementations are expected to raise a ValueError with a 
            descriptive error message to indicate that they received an 
            md_entity_type that is not in 
            metadata_lib.definitions.allowed_values.ENTITIES
        """
        pass


    @abstractmethod
    def write(
        self,
        location: str | dict[any,any],
        md_entity_type: str,
        entity_list: list[ dict[str,any] ]
    ) -> None:
        """Write the single json file of specified entity type to list of dicts.

        Parameters
        ----------
        location : str | dict[any,any]
            The information needed to address the location. Whether a str or 
            dict is used can ben choosen by the implementer. Location should 
            contain any and all information necessary for the storage_adapter 
            to access the storage.
        md_entity_type : str
            The entity type to write. Implementations are expected to 
            check that the entity_type is a valid entity_type according to 
            metadata_lib.definitions.allowed_values.ENTITIES.
        entity_list : list[ dict[str,any] ]
            A list of dicts where each dict is an entity of the specified type.
            The dicts should be json compatible. Implementations are 
            expected to focus on IO. Value conversion to json compatibility 
            should be implemented elsewhere. Specifically, we suggest running 
            entities through FastAPI's json_compatible_encoder before passing 
            them to a storage adapter for writing.
        
        Raises
        ------
        TypeError
            Implementations are expected to raise a TypeError with descriptive 
            error message to indicate that they received location in an 
            unexpected format (ie received a str, expected a dict).
        ValueError
            Implementations are expected to raise a ValueError with a 
            descriptive error message to indicate that they received an 
            md_entity_type that is not in 
            metadata_lib.definitions.allowed_values.ENTITIES
        """
        pass