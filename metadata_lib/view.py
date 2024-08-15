"""view.py
    Functions to present metadata to data engineers in an easily 
    understandable output format
"""
import pandas as pd




def all_ids_to_unids(
    metadata_id_idx: dict[ str,list[any] ]
) -> dict[ str,list[any] ]:
    """Take metadata in object form that has not yet been deeply integrated.
        Replace ALL integer id's with their respective UNIDs for the purpose 
        of making metadata readable
    
    Parameters
    ----------
    metadata_id_idx : dict[ str,list[any] ]
        Metadata in its by_id form
    
    Returns
    -------
    md_out : dict[ str,list[any] ]
        Metadata in the form of md_objects, but with all id's and all 
        integer references replaced with their unids.
    """
    # Output object
    md_out = {}

    # Some simpler names
    ns_byid = metadata_id_idx['namespaces_idx']
    sys_byid = metadata_id_idx['systems_idx']
    ent_byid = metadata_id_idx['data_entities_idx']
    pl_byid = metadata_id_idx['pipelines_idx']
    sch_byid = metadata_id_idx['schemas_idx']

    ## Replace all integer id references with their respective UNIDs
    # namespaces has no id references. Add them cleanly
    md_out['namespaces'] = []
    for ns in metadata_id_idx['namespaces_idx'].values():
        md_out['namespaces'].append(ns)
    # schemas
    md_out['schemas'] = []
    for sch in metadata_id_idx['schemas_idx'].values():
        sch.namespace = ns_byid[sch.namespace].unid
        md_out['schemas'].append(sch)
    # systems
    md_out['systems'] = []
    for sys in metadata_id_idx['systems_idx'].values():
        sys.namespace = ns_byid[sys.namespace].unid
        md_out['systems'].append(sys)
    # data_entities
    md_out['data_entities'] = []
    for ent in metadata_id_idx['data_entities_idx'].values():
        ent.namespace = ns_byid[ent.namespace].unid
        ent.system = sys_byid[ent.system].unid
        ent.entity_schema = sch_byid[ent.entity_schema].unid
        md_out['data_entities'].append(ent)
    # pipelines
    md_out['pipelines'] = []
    for pl in metadata_id_idx['pipelines_idx'].values():
        # Set namespace to UNID
        pl.namespace = ns_byid[pl.namespace].unid
        if isinstance(pl.input_output, list):
            # Compound. Iterate over the pipeline's instances
            for i, instance in enumerate(pl.input_output):
                ## Deal with input entities in the current instance
                if isinstance(instance['input'], list):
                    # List style input instance, multiple input entities. Iterate.
                    for j, inent in enumerate(instance['input']):
                        pl.input_output[i]['input'][j] = ent_byid[inent].unid
                else:
                    # Single entity input. Just assign
                    pl.input_output[i]['input'] = ent_byid[instance['input']].unid
                ## Deal with output entities in the current instance
                if isinstance(instance['output'], list):
                    # List style output instance, multiple output entities. Iterate.
                    for j, outent in enumerate(instance['output']):
                        pl.input_output[i]['output'][j] = ent_byid[outent].unid
                else:
                    # Single entity output. Just assign
                    pl.input_output[i]['output'] = ent_byid[instance['output']].unid
        else:
            # Single scope pipeline
            if isinstance(pl.input_output['input'], list):
                # List style input instance, multiple input entities. Iterate.
                for j, inent in enumerate(pl.input_output['input']):
                    pl.input_output['input'][j] = ent_byid[inent].unid
            else:
                # Single entity input. Just assign
                pl.input_output['input'] = ent_byid[pl.input_output['input']].unid
            ## Deal with output entities in the current instance
            if isinstance(pl.input_output['output'], list):
                # List style output instance, multiple output entities. Iterate.
                for j, outent in enumerate(pl.input_output['output']):
                    pl.input_output['output'][j] = ent_byid[outent].unid
            else:
                # Single entity output. Just assign
                pl.input_output['output'] = ent_byid[pl.input_output['output']].unid
        
        md_out['pipelines'].append(pl)
    
    return md_out



def metadata_obj_to_records(
    metadata_obj: dict[ str, list[any] | dict[any,any] ]
) -> dict[ str,list[any] ]:
    """Unpack objectified metadata to a dict of lists of dicts

    Pydantic objects will be unpacked to their dict form.

    Parameters
    ----------
    metadata_obj : dict[ str, list[any] | dict[any,any] ]
        Metadata in its md_objects form.
    
    Returns
    md_records : dict[ str,list[any] ]
        A form of metadata that is prepared to be read into a DataFrame
    """
    # Output object
    md_records = {}

    for key, value in metadata_obj.items():
        if isinstance(value, list):
            # Metadata_obj style received. Value is a list to iterate
            md_records[key] = []
            for ent_obj in value:
                md_records[key].append(ent_obj.model_dump())
        elif isinstance(value, dict):
            # metadata_idx style received. Value is a dict
            ent_type = key.replace('_idx', '')
            md_records[ent_type] = []
            for ent_obj in value.values():
                md_records[ent_type].append(ent_obj.model_dump())
    
    return md_records

    

def metadata_records_to_dataframes(
    metadata_records: dict[ str,list[any] ]
) -> dict[str, pd.DataFrame]:
    """Turn plain metadata into dataframes for display

    Parameters
    ----------
    metadata_records : dict[ str,list[any] ]
        Metadata as it is output by metadata_obj_to_records
    
    Returns
    -------
    md_df : dict[str, pandas.DataFrame]
        A dict of Dataframes where each dataframe is a collection
        of metadata entities.
    """
    # Output object
    md_df = {}

    for ent_type, ent_list in metadata_records.items():
        md_df[ent_type] = pd.DataFrame.from_records(ent_list)
    
    return md_df