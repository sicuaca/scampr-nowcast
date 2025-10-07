import xarray as xr
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
from matplotlib.colors import ListedColormap, BoundaryNorm
from datetime import datetime, timedelta
import yaml
import os
import argparse

LATEST_NOWCAST_INFO = 'D:\\Projects\\scampr-nowcasting\\data\\{domain}\\latest_nowcast_available.json'


def read_config(config: os.PathLike | str) -> dict:
    required_keys = ['domain', 'png_output_storage_dir']
    try:
        with open(config, 'r') as f:
            cfg = yaml.safe_load(f)
        for key in required_keys:
            if key not in cfg:
                print(f"Error: key '{key}' not found in config.")
    except FileNotFoundError:
        raise
    return cfg


def plot_data(da: xr.DataArray, output_file: str = None):
    data = da.fillna(0)

    vmin, vmax = float(data.min()), float(data.max())
    # contoh: gunakan level non-linear jika curah hujan
    levels = [0.1, 1.0, 2.0, 5.0, 7.0, 9.0, 10, 12, 15, 20, 50, 100]
    colors = [
        "#0000c7", "#0079ff", "#32c8ff", "#78ebff",
        "#ffffff", "#fff7c0", "#ffe500", "#ff7300",
        "#ff3f00", "#c80000", "#960000", "#6e0000"
    ]
    colors = ["#ffffff00"] + colors
    levels = [0.0] + levels

    # Buat colormap & normalization
    cmap = ListedColormap(colors)
    norm = BoundaryNorm(levels, ncolors=cmap.N, clip=True)

    lon = data["lon"]
    lat = data["lat"]

    fig, ax = plt.subplots(figsize=(8, 6))

    # Plot pcolormesh agar presisi spasial
    mesh = ax.pcolormesh(
        lon, lat, data,
        cmap=cmap, norm=norm,
        shading="auto"
    )

    # Matikan axis untuk layer peta
    ax.axis("off")

    extent = [float(lon.min()), float(lon.max()), float(lat.min()), float(lat.max())]
    ax.set_xlim(extent[0], extent[1])
    ax.set_ylim(extent[2], extent[3])

    # output_file = f"scampr_steps_{domain}_base{timestamp:%Y%m%d%H%M}_valid{timestamp:%Y%m%d%H%M}.png"
    plt.savefig(
        output_file,
        bbox_inches="tight",
        pad_inches=0,
        transparent=True,
        dpi=150
    )
    plt.close()


def generate_png_layer(config: os.PathLike | str | dict):
    if isinstance(config, (str, os.PathLike)):
        cfg = read_config(config)
    elif isinstance(config, dict):
        cfg = config
    else:
        raise ValueError("config must be a file path or a dictionary")

    domain = cfg['domain'].lower()
    png_storage_dir = cfg.get('png_output_storage_dir', None).format(domain=domain)

    latest_nowcast_file = LATEST_NOWCAST_INFO.format(domain=domain)
    with open(latest_nowcast_file, 'r') as f:
        latest_info = yaml.safe_load(f)

    base_time = latest_info['base_time']
    file_path = latest_info['file_path']

    ds = xr.open_dataset(file_path, engine="netcdf4")
    var = 'mean_rr'
    if 'time' in ds.dims:
        times = ds['time'].values
        for t in times:
            ds_time = ds.sel(time=t)
            data = ds_time[var]
            leadtime = data.leadtime.values
            timestamp = datetime.strptime(str(data.time.values), "%Y-%m-%dT%H:%M:%S.%f000")
            timestamp = timestamp.strftime("%Y%m%d%H%M000")
            os.makedirs(png_storage_dir, exist_ok=True)
            output_file = f"scampr_steps_{domain}_base{base_time}_valid{timestamp}_{leadtime}.png"
            print("Generating:", output_file)
            plot_data(data, os.path.join(png_storage_dir, output_file))

    else:
        print("time dimension not found in dataset.")
        return

    print("PNG generation completed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate PNG layers from nowcast netCDF files.")
    parser.add_argument("-c", "config", type=str, help="Path to the configuration YAML file.")
    args = parser.parse_args()

    generate_png_layer(args.config)
