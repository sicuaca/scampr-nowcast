from download_scampr import download_scampr
from convert_tiff import convert_tiff
from run_nowcasting import run_nowcasting

from datetime import datetime, timedelta, UTC
import yaml
import json
import os
import argparse

DOMAIN_DICT = 'D:\\Projects\\scampr-nowcasting\\domain_boundary.yaml'
LATEST_FILE_INFO = 'D:\\Projects\\scampr-nowcasting\\data\\latest_file_available.json'
TIF_FILE_LIST = 'D:\\Projects\\scampr-nowcasting\\data\\tif\\{domain}\\tif_file_list.json'


def read_config(config:os.PathLike|str)->dict:
    required_keys = ['nc_storage_dir','nc_filename_template','domain','prior_steps','model_config','tif_storage_dir','tif_filename_template','nowcast_output_storage_dir','nowcast_output_filename_template']
    try:
        with open(config,'r') as f:
            cfg = yaml.safe_load(f)
        for key in required_keys:
            if key not in cfg:
                print(f"Error: key '{key}' not found in config.")
    except FileNotFoundError:
        raise
    return cfg


def main(config: os.PathLike | str, time: str = None):
    print("Starting SCAMPR Nowcasting Pipeline...")
    cfg = read_config(config)
    run_mode = cfg.get('run_mode', 'auto')
    tif_storage_dir = cfg['tif_storage_dir']
    tif_filename_template = cfg['tif_filename_template']
    domain = cfg['domain']
    prior_steps = cfg['prior_steps']

    if run_mode == 'auto':
        if time:
            print("Running in auto mode. Getting base time from arguments.")
            base_time = datetime.strptime(time, '%Y%m%d%H%M').replace(tzinfo=UTC)
        else:
            print("Running in auto mode. Getting base time from latest available file.")
            with open(LATEST_FILE_INFO, 'r') as f:
                latest_file_info = json.load(f)
                latest_file = latest_file_info['file_path']
                base_time = datetime.strptime(latest_file_info['time_coverage_start'], '%Y%m%d%H%M000')

    elif run_mode == 'manual':
        if time:
            print("Running in manual mode. Getting base time from arguments.")
            base_time = datetime.strptime(time, '%Y%m%d%H%M').replace(tzinfo=UTC)
        else:
            print("Running in manual mode. Getting base time from current UTC time.")
            base_time = datetime.now(UTC)
            base_time = base_time.replace(minute=(base_time.minute // 10) * 10, second=0, microsecond=0)

    else:
        raise ValueError(f"Invalid run_mode: {run_mode}. Must be 'auto' or 'manual'.")

    # Make time list based on latest file available and prior steps 10 minutes each
    time_list = [base_time - timedelta(minutes=10 * i) for i in range(prior_steps)]
    time_list = sorted(time_list)

    # Check if the tif files already exist
    print("Checking for existing tif files...")
    tif_files = []
    for t in time_list:
        tif_filename = tif_filename_template.format(domain=domain.lower(), datestring=t.strftime('%Y%m%d%H%M000'))
        tif_dir = tif_storage_dir.format(domain=domain.lower())
        tif_filepath = os.path.join(tif_dir, tif_filename)
        tif_files.append(tif_filepath)

    if all([os.path.exists(f) for f in tif_files]):
        print("All tif files already exist.")
    else:
        # check missing tif files
        missing_tif_files = [f for f in tif_files if not os.path.exists(f)]
        # take time string from filename
        missing_time_list = [os.path.basename(f).split('_')[2].split('.')[0] for f in missing_tif_files]
        print(f"Missing {len(missing_tif_files)} tif files. Proceeding to download and convert...")
        for t, f in zip(missing_time_list, missing_tif_files):
            try:
                print(f"Downloading and converting for time: {t}")
                download_scampr(cfg, t)
                convert_tiff(cfg, t)
            except Exception as e:
                print(f"{t} skipped due to error: {e}")
                # hapus dari tif_files jika gagal
                if f in tif_files:
                    tif_files.remove(f)

    print(f"Tif files ready: {tif_files}")
    # Save the tif file list to a json file
    print("Saving tif file list...")
    os.makedirs(os.path.dirname(TIF_FILE_LIST.format(domain=domain.lower())), exist_ok=True)
    tif_file_list_path = TIF_FILE_LIST.format(domain=domain.lower())
    with open(tif_file_list_path, 'w') as f:
        json.dump(tif_files, f, indent=4)

    print("Running nowcasting model...")
    ds = run_nowcasting(cfg, tif_file_list_path, processed_output=True)
    return ds


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SCAMP Nowcasting Pipeline")
    parser.add_argument('-c','--config', type=str, required=True, help='Path to configuration YAML file')
    parser.add_argument('-t','--time', type=str, default=None, help='Optional time string in YYYYMMDDHHMM format')
    args = parser.parse_args()

    main(args.config, args.time)