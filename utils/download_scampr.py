#!/home/metpublic/PYTHON_VENV/nowcasting_weather/bin/python
import os
try:
    from read_config import read_run_config
except ModuleNotFoundError:
    from utils.read_config import read_run_config

import argparse
import boto3
import yaml
import json
from botocore import UNSIGNED
from botocore.config import Config

from datetime import datetime, timedelta
from datetime import UTC
import io
import xarray as xr
import numpy as np


def transform_data(data, clip=None):
    ds = xr.open_dataset(data, engine='h5netcdf')
    # Original dimension
    nrows = ds.sizes["Rows"]  # atau "row"
    ncols = ds.sizes["Columns"]  # atau "column"

    # metadata
    lat_min = float(ds.geospatial_lat_min)
    lat_max = float(ds.geospatial_lat_max)
    lon_min = float(ds.geospatial_lon_min)
    lon_max = float(ds.geospatial_lon_max)

    # generate grids coordinates
    lats = np.linspace(lat_max, lat_min, nrows)  # north → south
    lons = np.linspace(lon_min, lon_max, ncols)

    # Assign koordinat
    ds = ds.assign_coords(
        lat=("Rows", lats),
        lon=("Columns", lons)
    )

    # Ganti dimensi: row→lat, column→lon
    ds = ds.swap_dims({"Rows": "lat", "Columns": "lon"})

    # Tambahkan atribut CF
    ds["lat"].attrs = {
        "standard_name": "latitude",
        "long_name": "latitude",
        "units": "degrees_north"
    }
    ds["lon"].attrs = {
        "standard_name": "longitude",
        "long_name": "longitude",
        "units": "degrees_east"
    }

    if clip:
        north, south, west, east = clip
        ds_clip = ds.sel(
            lon=slice(west, east),
            lat=slice(north, south))
        ds_clip.attrs = ds.attrs.copy()

        ds_clip.attrs['geospatial_lat_min'] = float(ds_clip.lat.min())
        ds_clip.attrs['geospatial_lat_max'] = float(ds_clip.lat.max())
        ds_clip.attrs['geospatial_lon_min'] = float(ds_clip.lon.min())
        ds_clip.attrs['geospatial_lon_max'] = float(ds_clip.lon.max())
        return ds_clip

    else:
        return ds


def get_latest_file(bucket, prefixes, substring:str|list="GLB-5"):
    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))

    for prefix in prefixes:
        print(f"Looking for data at {prefix}")
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        contents = response.get("Contents", [])
        if not contents:
            continue  # coba prefix berikutnya

        # filter berdasarkan substring
        if isinstance(substring, list):
            filtered = [obj for obj in contents if all(sub in obj["Key"] for sub in substring)]
        else:
            filtered = [obj for obj in contents if substring in obj["Key"]]

        if filtered:
            # ambil objek terbaru tanpa sort full list
            latest = max(filtered, key=lambda x: x["LastModified"])
            return latest
    return None


def download_scampr(config: dict| str | os.PathLike, time: str = None):
    now = datetime.now(UTC)
    now = now.replace(minute=(now.minute // 10) * 10, second=0, microsecond=0)
    now_1 = now - timedelta(hours=1)
    print(f"Initializing download at {now:%m-%d %H:%M}")

    if isinstance(config, dict):
        cfg = config
    else:
        cfg = read_run_config(config)

    bucket_name = cfg.get('bucket_name')
    clip = cfg.get('clip')
    prefix = cfg.get('prefix').format(datestring=now.strftime('%Y/%m/%d/%H'))
    prefix_1 = cfg.get('prefix').format(datestring=now_1.strftime('%Y/%m/%d/%H'))
    local_dir = cfg.get('nc_dir')

    if time:
        time_dt = datetime.strptime(time, "%Y%m%d%H%M000")
        prefix = cfg.get('prefix').format(datestring=time_dt.strftime('%Y/%m/%d/%H'))
        latest_obj = get_latest_file(bucket_name, [prefix], [time,'GLB-5'])
    else:
        prefixes = [prefix, prefix_1]
        latest_obj = get_latest_file(bucket_name, prefixes)

    if latest_obj:
        print(f"Found requested time: {latest_obj['Key']}")
        print(f"Processing data: {latest_obj['Key']}")
        filename_aws = os.path.basename(latest_obj["Key"])
        timestamp = filename_aws.split("_")[3][1:]

        if clip:
            filename_check = cfg.get('nc_filename_template').format(datestring=timestamp)
            local_file = os.path.join(local_dir, filename_check)
        else:
            local_file = os.path.join(local_dir, filename_aws)

        s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))

        # Cek apakah file lokal sudah ada
        if os.path.isfile(local_file):
            file_size = os.path.getsize(local_file)

            # Jika file kecil, re-download
            if file_size < 700 * 1024:
                print(f"File size {file_size} bytes is less than 700KB, re-downloading...")
                obj = s3.get_object(Bucket=bucket_name, Key=latest_obj["Key"])
                data = io.BytesIO(obj["Body"].read())
            else:
                print(f"File already exists: {local_file}, skipping download.")
                return
        else:
            # File belum ada, download
            print(f"File not found: {local_file}, downloading...")
            obj = s3.get_object(Bucket=bucket_name, Key=latest_obj["Key"])
            data = io.BytesIO(obj["Body"].read())

    else:
        raise FileNotFoundError("No matching files found")


    print("Transforming data to xarray dataset")
    ds = transform_data(data, clip)

    print("Saving to netcdf")
    file_datestring = datetime.strptime(ds.attrs['time_coverage_start'], "%Y-%m-%dT%H:%M:%SZ").strftime("%Y%m%d%H%M000")

    filename = cfg.get('nc_filename_template').format(datestring=file_datestring)
    os.makedirs(local_dir, exist_ok=True)
    output_file = os.path.join(local_dir, filename)
    ds.to_netcdf(output_file, format='NETCDF4', engine='netcdf4')

    if not time:
        print("Writing latest_file_available.json")
        latest_info = {
            "latest_filename": filename,
            "file_path": output_file,
            "time_coverage_start": file_datestring,
        }
        latest_file = cfg.get('nc_latest_file_info',os.path.join(local_dir, "latest_file_available.json"))
        with open(latest_file, 'w') as f:
            json.dump(latest_info, f, indent=4)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download SCaMPR data from AWS S3")
    parser.add_argument('-c', '--config', type=str, required=True, help='Path to config file')
    parser.add_argument('-t', '--time', type=str, required=False, help='Time in format YYYYMMDDHHMM to download specific file')
    args = parser.parse_args()
    if args.time:
        download_scampr(args.config, args.time)
    else:
        download_scampr(args.config)
