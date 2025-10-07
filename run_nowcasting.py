from pysteps import nowcasts
from pysteps.motion.lucaskanade import dense_lucaskanade
from pysteps.utils import transformation

import os
import yaml
import numpy as np
import rasterio
import json
from datetime import datetime, UTC, timedelta
import xarray as xr
import argparse

DOMAIN_DICT = 'D:\\Projects\\scampr-nowcasting\\domain_boundary.yaml'
LATEST_FILE_INFO = 'D:\\Projects\\scampr-nowcasting\\data\\latest_file_available.json'
TIF_FILE_LIST = 'D:\\Projects\\scampr-nowcasting\\data\\tif\\{domain}\\tif_file_list.json'


def read_config(config: os.PathLike | str) -> dict:
    required_keys = ['domain', 'model_config', 'tif_storage_dir', 'tif_filename_template']
    try:
        with open(config, 'r') as f:
            cfg = yaml.safe_load(f)
        for key in required_keys:
            if key not in cfg:
                print(f"Error: key '{key}' not found in config.")
    except FileNotFoundError:
        raise
    return cfg


def convert_to_dataset(data: np.ndarray, metadata: dict, base_time: datetime, timestep: int,
                       km_per_pixel: int | None) -> xr.Dataset:
    ens_index = [i + 1 for i in range(data.shape[0])]
    time_index = [base_time + timedelta(minutes=timestep * i) for i in range(1,data.shape[1]+1)]
    leadtime_index = [timestep * i for i in range(1,data.shape[1]+1)]
    nx = data.shape[3]
    ny = data.shape[2]

    ds = xr.Dataset(
        {
            "rr": (("member", "time", "lat", "lon"), data)
        },
        coords={
            "time": time_index,
            "member": ens_index,
            "leadtime": ("time", leadtime_index),
            "lon": np.linspace(round(metadata['geodata']['x1'], 1), round(metadata['geodata']['x2'], 1), nx),
            "lat": np.linspace(round(metadata['geodata']['y2'], 1), round(metadata['geodata']['y1'], 1), ny)
        },
        attrs={
            "projection": metadata['geodata']['projection'],
            "x1": round(metadata['geodata']['x1'], 1),
            "y1": round(metadata['geodata']['y1'], 1),
            "x2": round(metadata['geodata']['x2'], 1),
            "y2": round(metadata['geodata']['y2'], 1),
            "yorigin": metadata['geodata']['yorigin'],
            "timestep_minutes": timestep,
            "km_per_pixel": km_per_pixel,
        }
    )
    return ds


def compute_ensemble(ds: xr.Dataset) -> xr.Dataset:
    mean = ds['rr'].mean("member")
    prob = (ds['rr'] >= 1.0).sum("member") / ds.member.size
    mean.attrs = {
        "units": "mm/h",
        "long_name": "Ensemble mean of rain rate"
    }

    prob.attrs = {
        "units": "1",
        "long_name": "Probability of exceeding 1mm/h"
    }

    ds_ens = xr.Dataset(
        {
            "mean_rr": mean,
            "prob_1mm": prob
        },
        coords=ds.coords.drop_dims('member'),
        attrs={
            "description": "Processed output from nowcasting ensemble, the output is transformed back to rain rate (mm/h).",
        }
    )
    return ds_ens


def run_nowcasting(config: os.PathLike | str|dict, tif_files: None | os.PathLike | str | list[str] = TIF_FILE_LIST,
                   processed_output=True) -> (str,xr.Dataset):

    #identify config input type
    if isinstance(config, dict):
        cfg = config
    else:
        cfg = read_config(config)

    domain = cfg['domain'].lower()
    model_config = cfg['model_config']

    # check tif_files argument input
    if tif_files is not None:
        if isinstance(tif_files, str):
            tif_file_list_path = tif_files.format(domain=domain)
            with open(tif_file_list_path, 'r') as f:
                tif_input_files = json.load(f)
        elif isinstance(tif_files, list):
            tif_input_files = tif_files
        else:
            raise ValueError(
                "tif_files argument must be a json file path string containing list of tif files path, or a list of file paths.")
    else:
        tif_file_list_path = TIF_FILE_LIST.format(domain=domain)
        with open(tif_file_list_path, 'r') as f:
            tif_input_files = json.load(f)

    base_time = os.path.basename(tif_input_files[-1]).split('_')[2].split('.')[0]
    base_time = datetime.strptime(base_time, '%Y%m%d%H%M000') + timedelta(minutes=10)

    R = []
    metadata = {}
    for file_path in tif_input_files:
        with rasterio.open(file_path) as ds:
            R.append(ds.read(1))
            if file_path == tif_input_files[-1]:
                metadata['geodata'] = {'projection': ds.crs.to_proj4(), 'x1': ds.bounds.left, 'y1': ds.bounds.bottom,
                                       'x2': ds.bounds.right, 'y2': ds.bounds.top, 'yorigin': 'upper'}
    R = np.stack(R)

    R, metadata_db = transformation.dB_transform(R, threshold=0.1, zerovalue=-15.0)
    R[~np.isfinite(R)] = -15.0

    n_input_frames = model_config['n_input_frames']
    if R.shape[0] < n_input_frames:
        n_input_frames = R.shape[0]

    V = dense_lucaskanade(R[-n_input_frames:, :, :])
    V[~np.isfinite(V)] = 0.0
    max_velocity = 100
    V[V > max_velocity] = max_velocity
    V[V < -max_velocity] = -max_velocity

    method = model_config.get('method', 'steps')
    n_leadtimes = model_config['n_leadtimes']
    n_ens_members = model_config['n_ens_members']
    km_per_pixel = model_config['km_per_pixel']
    timestep = model_config['timestep']
    precip_thr = model_config.get('precip_thr', -10.0)

    if method == 'steps':
        steps = nowcasts.get_method(method)
        R_f = steps(
            R, V, n_leadtimes, n_ens_members,
            kmperpixel=km_per_pixel, timestep=timestep, precip_thr=precip_thr,
            seed=42, extrap_kwargs={'boundary_condition': 'zero'},
            noise_method='parametric', ar_order=1
        )

    R_f = transformation.dB_transform(R_f, threshold=-10.0, inverse=True)[0]

    ds = convert_to_dataset(R_f, metadata, base_time, timestep, km_per_pixel)
    if processed_output:
        ds = compute_ensemble(ds)

    output_path = cfg.get('nowcast_output_storage_dir')
    output_path = output_path.format(domain=domain.lower())
    os.makedirs(output_path, exist_ok=True)
    filename = cfg.get('nowcast_output_filename_template')
    filename = filename.format(method=method, domain=domain.lower(), base_time=base_time.strftime('%Y%m%d%H%M'))
    #nc compression
    comp = dict(zlib=True, complevel=8)
    encoding = {var: comp for var in ds.data_vars}

    ds.to_netcdf(os.path.join(output_path, filename), format='NETCDF4', encoding=encoding, engine='netcdf4')
    return os.path.join(output_path, filename), ds


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Run nowcasting using pysteps.")
    parser.add_argument('-c','--config', type=str, required=True, help="Path to the configuration YAML file.")
    parser.add_argument('--tif_files', type=str, default=None,
                        help="Path to JSON file containing list of GeoTIFF files or a comma-separated list of file paths. If not provided, it will use the default path in the script.")
    parser.add_argument('--processed_output', action='store_true',
                        help="If set, the output will not be processed to ensemble mean and probability.")
    args = parser.parse_args()

    tif_files_input = None
    if args.tif_files:
        if args.tif_files.endswith('.json'):
            tif_files_input = args.tif_files
        else:
            tif_files_input = args.tif_files.split(',')

    ds_nowcast = run_nowcasting(config=args.config, tif_files=tif_files_input,
                                processed_output=not args.processed_output)
    print(ds_nowcast)
