import os
import yaml


def read_run_config(config:os.PathLike|str)->dict:
    required_keys = ['bucket_name', 'prefix', 'product', 'nc_filename_template']
    try:
        with open(config,'r') as f:
            cfg = yaml.safe_load(f)
        for key in required_keys:
            if key not in cfg:
                print(f"Error: key '{key}' not found in config.")
    except FileNotFoundError:
        raise
    return cfg


def read_path_config(config:os.PathLike|str)->dict:
    required_keys = ['project_path','config_path','status_path','data_path','log_path',
                     'nc_latest_file_info','tif_file_list_info','latest_nowcast_info','latest_png_info',
                     'nc_dir','tif_dir','nowcast_dir','png_layer_dir']
    try:
        with open(config,'r') as f:
            cfg = yaml.safe_load(f)
        for key in required_keys:
            if key not in cfg:
                print(f"Error: key '{key}' not found in config.")
    except FileNotFoundError:
        raise
    return cfg