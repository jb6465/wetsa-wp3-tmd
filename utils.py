# directories
barraR2_dir = '/g/data/ob53/BARRA2/output/reanalysis/AUS-11/BOM/ERA5/historical/hres/BARRA-R2/v1/'
era5_dir = '/g/data/rt52/era5/'

domain_dict = { #can add nation domains for future wetsa work
    'indonesia':{'lat_min':-11, 'lat_max':6, 'lon_min':95, 'lon_max':141},
    'thailand':{'lat_min':5, 'lat_max':21, 'lon_min':97, 'lon_max':106},
    'australia':{'lat_min':-44, 'lat_max':-10, 'lon_min':112, 'lon_max':154},
}

#functions
def barra_extract(target_var, extracted_data_save_dir, time_step, nation_domain, year):
    """
    Inputs:
    - target_var - a string of BARRA-R2 target variable key
    - extracted_data_save_dir - string of directory to save extracted data in
    - time_step - string to specify time resolution of BARRA-R2 data [10min 1hr 3hr day fx mon]
    - nation_domain - string to specify target nation domain 
    - year - string of target extraction year
    Returns:
    Yearly files saved in specified dir for chosen target variable and timestep 
    """
    
    import os
    import glob
    import xarray as xr
    
    if not os.path.isfile(f"{extracted_data_save_dir}/BARRAR2/{time_step}/{target_var}/{nation_domain}_BARRAR2_{year}_{target_var}_{time_step}.nc"):
        os.makedirs(os.path.abspath(f"{extracted_data_save_dir}/BARRAR2/{time_step}/{target_var}/"), exist_ok=True)
        barra_files = sorted(glob.glob(f"{barraR2_dir}/{time_step}/{target_var}/latest/*{year}*-{year}*.nc"))
        datasets = []
        for file in barra_files:
            ds = xr.open_dataset(file, engine='netcdf4')
            datasets.append(ds[target_var].sel(lat=slice(domain_dict[nation_domain]['lat_min'], domain_dict[nation_domain]['lat_max']), lon=slice(domain_dict[nation_domain]['lon_min'], domain_dict[nation_domain]['lon_max'])))
            del ds
        ds_cube = xr.concat(datasets, dim='time')
        ds_cube.to_netcdf(f"{extracted_data_save_dir}/BARRAR2/{time_step}/{target_var}/{nation_domain}_BARRAR2_{year}_{target_var}_{time_step}.nc", encoding={f"{target_var}": {'zlib': True, 'complevel': 5, 'dtype':'float32'}})
        ds_cube.close()
    return

def era5_extract(target_var, extracted_data_save_dir, nation_domain, year):
    """
    Inputs:
    - target_var - a string of ERA5 target variable key
    - extracted_data_save_dir - string of directory to save extracted data in
    - nation_domain - string to specify target nation domain 
    - year - string of target extraction year
    Returns:
    Yearly files saved in specified dir for chosen target variable and timestep 
    """
    import os
    import glob
    import xarray as xr

    if not os.path.isfile(f"{extracted_data_save_dir}/ERA5/{target_var}/{nation_domain}_ERA5_{year}_{target_var}.nc"):
        os.makedirs(f"{extracted_data_save_dir}/ERA5/{target_var}/", exist_ok=True)
        
        def preprocess_era5(ds):
            return ds.sel(latitude=slice(domain_dict[nation_domain]['lat_max'], domain_dict[nation_domain]['lat_min']), longitude=slice(domain_dict[nation_domain]['lon_min'], domain_dict[nation_domain]['lon_max']))
        
        era5_files = sorted(glob.glob(f"{era5_dir}single-levels/reanalysis/{target_var}/{year}/*.nc"))
        ds_cube = xr.open_mfdataset(era5_files,combine='by_coords',parallel=True, engine='netcdf4', preprocess=preprocess_era5).chunk({'time':-1, 'latitude':'auto', 'longitude':'auto'})
        ds_cube.to_netcdf(f"{extracted_data_save_dir}/ERA5/{target_var}/{nation_domain}_ERA5_{year}_{target_var}.nc", encoding={f"{list(ds_cube.data_vars)[0]}": {'zlib': True, 'complevel': 5, 'dtype':'float32'}})
        ds_cube.close()
    return
