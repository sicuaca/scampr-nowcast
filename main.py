from utils.download_scampr import download_scampr
from utils.convert_tiff import convert_tiff
from utils.run_nowcasting import run_nowcasting
from utils.generate_png_layer import generate_png_layer
from utils.read_config import read_run_config, read_path_config

from datetime import datetime, timedelta, UTC
import yaml
import json
import os
import argparse


def main(config: os.PathLike | str, time: str = None):
    cfg = read_run_config(config)
    domain_dict = cfg.get('domain_info')
    nc_dir = cfg.get('nc_dir')
    nc_latest_file_info = cfg.get('nc_latest_file_info')
    tif_file_list_info = cfg.get('tif_file_list_info')
    tif_storage_dir = cfg.get('tif_dir')
    latest_nowcast_info = cfg.get('latest_nowcast_info')

    run_mode = cfg.get('run_mode', 'auto')
    tif_filename_template = cfg['tif_filename_template']
    domain = cfg['domain']
    prior_steps = cfg['prior_steps']

    if run_mode == 'auto':
        if time:
            print("Running in auto mode. Getting base time from arguments.")
            base_time = datetime.strptime(time, '%Y%m%d%H%M').replace(tzinfo=UTC)
        else:
            print("Running in auto mode. Getting base time from latest available file.")
            with open(nc_latest_file_info, 'r') as f:
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
        print("Checking for missing files")
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
    #check latest tif file time and modify base time
    latest_tif_time_str = os.path.basename(tif_files[-1]).split('_')[2].split('.')[0]
    latest_tif_time = datetime.strptime(latest_tif_time_str, '%Y%m%d%H%M000').replace(tzinfo=UTC)
    if latest_tif_time != base_time:
        print(f"Adjusting base_time from {base_time} to {latest_tif_time} based on latest tif file.")
        base_time = latest_tif_time

    #check if at least 3 tif files are in sequence
    tif_times = [datetime.strptime(os.path.basename(f).split('_')[2].split('.')[0], '%Y%m%d%H%M000').replace(tzinfo=UTC) for f in tif_files]
    tif_times_sorted = sorted(tif_times)
    time_diffs = [(tif_times_sorted[i] - tif_times_sorted[i-1]).total_seconds() / 60 for i in range(1, len(tif_times_sorted))]

    if not all([diff == 10 for diff in time_diffs[-(prior_steps-1):]]):
        raise ValueError("Tif files are not in sequence of 10 minutes interval. Please check the available tif files.")

    # Save the tif file list to a json file
    print("Saving tif file list...")
    tif_file_list_info = tif_file_list_info.format(domain=domain.lower())
    with open(tif_file_list_info, 'w') as f:
        json.dump(tif_files, f, indent=4)

    print("Running nowcasting model...")
    output_file,ds = run_nowcasting(cfg, tif_file_list_info, processed_output=True)
    if ds:
        print("Nowcasting completed successfully.")

        print("Saving latest_nowcast_available.json...")
        latest_nowcast = {
            'base_time': (base_time+timedelta(minutes=10)).strftime('%Y%m%d%H%M000'),
            'file_path': output_file,
        }

        latest_nowcast_info = latest_nowcast_info.format(domain=domain.lower())
        with open(latest_nowcast_info, 'w') as f:
            json.dump(latest_nowcast, f, indent=4)

    else:
        print("Nowcasting failed.")

    generate_png_layer(cfg)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SCAMP Nowcasting Pipeline")
    parser.add_argument('-c','--config', type=str, required=True, help='Path to configuration YAML file')
    parser.add_argument('-t','--time', type=str, default=None, help='Optional time string in YYYYMMDDHHMM format')
    args = parser.parse_args()

    main(args.config, args.time)