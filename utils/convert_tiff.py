#!/home/metpublic/PYTHON_VENV/nowcasting_weather/bin/python
import xarray
from datetime import datetime, timedelta
import json
import yaml
import os
import argparse
try:
    from read_config import read_run_config
except ModuleNotFoundError:
    from utils.read_config import read_run_config

def read_domain_dictionary(domain: os.PathLike | str) -> dict:
    try:
        with open(domain, 'r') as f:
            domain_dict = yaml.safe_load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"Domain dictionary file not found at {domain}")
    return domain_dict


def convert_tiff(config: dict | str | os.PathLike, time: str = None):
    print("Converting NetCDF to GeoTIFF...")

    if isinstance(config, dict):
        cfg = config
    else:
        cfg = read_run_config(config)

    nc_dir = cfg.get('nc_dir')
    nc_filename = cfg.get('nc_filename_template')
    tif_dir = cfg.get('tif_dir')
    tif_filename = cfg.get('tif_filename_template')
    domain = cfg.get('domain', 'Indonesia')
    nc_latest_file_info = cfg.get('nc_latest_file_info')

    domain_info = cfg.get("domain_info")
    domain_dict = read_domain_dictionary(domain_info)
    boundary = domain_dict.get(domain).get('boundary')

    if not time:
        try:
            with open(nc_latest_file_info) as f:
                latest_file_available = json.load(f)
                latest_file_path = latest_file_available.get('file_path')
        except FileNotFoundError:
            raise FileNotFoundError(f"Latest file info not found at {nc_latest_file_info}")
    else:
        try:
            file_datestring = datetime.strptime(time, '%Y%m%d%H%M000').strftime('%Y%m%d%H%M000')
        except ValueError:
            file_datestring = datetime.strptime(time, '%Y%m%d%H%M').strftime('%Y%m%d%H%M000')
        latest_file_path = f"{nc_dir}/{nc_filename.format(datestring=file_datestring)}"

    print(f"Processing file: {latest_file_path}")
    ds = xarray.open_dataset(latest_file_path, engine='netcdf4')
    ds_clip = ds.sel(lat=slice(boundary[0], boundary[1]), lon=slice(boundary[2], boundary[3]))
    sliced = ds_clip['RRQPE'].squeeze()
    sliced = sliced.rio.write_crs("EPSG:4326")
    sliced = sliced.rio.set_spatial_dims(x_dim="lon", y_dim="lat", inplace=True)

    # Write some attributes
    print("Writing attributes...")
    sliced.attrs['time_coverage_start'] = ds_clip.attrs['time_coverage_start']
    sliced.attrs['time_coverage_end'] = ds_clip.attrs['time_coverage_end']
    sliced.attrs['geospatial_lat_min'] = round(float(ds_clip.lat.min()), 2)
    sliced.attrs['geospatial_lat_max'] = round(float(ds_clip.lat.max()), 2)
    sliced.attrs['geospatial_lon_min'] = round(float(ds_clip.lon.min()), 2)
    sliced.attrs['geospatial_lon_max'] = round(float(ds_clip.lon.max()), 2)
    sliced.attrs['geospatial_lat_units'] = 'degrees_north'
    sliced.attrs['geospatial_lon_units'] = 'degrees_east'
    sliced.attrs['geospatial_lat_resolution'] = ds_clip.attrs.get('geospatial_lat_resolution')
    sliced.attrs['geospatial_lon_resolution'] = ds_clip.attrs.get('geospatial_lon_resolution')

    # Save to GeoTIFF
    print("Saving to GeoTIFF...")
    file_datestring = datetime.strptime(ds.attrs['time_coverage_start'], "%Y-%m-%dT%H:%M:%SZ").strftime("%Y%m%d%H%M000")
    filename = tif_filename.format(domain=domain.lower(), datestring=file_datestring)
    tif_dir = tif_dir.format(domain=domain.lower())
    os.makedirs(tif_dir, exist_ok=True)
    tif_file = f"{tif_dir}/{filename}"
    sliced.rio.to_raster(tif_file, compression='LZW', dtype='float32')
    print(f"GeoTIFF saved to: {tif_file}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Convert NetCDF to GeoTIFF")
    parser.add_argument('-c', '--config', type=str, required=True, help='Path to the configuration YAML file')
    parser.add_argument('-t', '--time', type=str, help='Optional time string in YYYYMMDDHHMM format')
    args = parser.parse_args()

    if args.time:
        convert_tiff(args.config, args.time)
    else:
        convert_tiff(args.config)
