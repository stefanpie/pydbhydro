from os import name

from numpy.ma.core import sort
from dbhydro import DBHYDRO

import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation

import cartopy.crs as ccrs
import cartopy.feature as cfeature
import numpy as np
import pandas as pd
from scipy.interpolate import griddata

import datetime

db = DBHYDRO()
# stations = db.get_all_stations(staion_name="0%")
# print(stations)
# wx_data = db.get_wx_data(start_date=datetime.date(2021, 9, 10), end_date=datetime.date(2021, 9, 10))
# wx_data.to_csv("./test_data.csv", index=False)

wx_data = pd.read_csv("./test_data.csv", parse_dates=["sample_dt"])
wx_data = wx_data[wx_data["data_type"] == "AIRT"]
# wx_data = wx_data[wx_data["sample_dt"].dt.minute % 15 == 0 ]
# wx_data = wx_data[wx_data["sample_dt"].dt.second == 0]


station_locations = wx_data.drop_duplicates(subset="station")[["station", "lat", "lon"]]

grid_lat, grid_lon,  = np.mgrid[24.5:28.3:100j, -83:-79.7:100j]
# print(grid_lon)
# print(grid_lat)

grid_data_t = []
for sample_dt, group_df in wx_data.groupby("sample_dt"):
    print(sample_dt)
    d_lat = group_df["lat"].to_numpy()
    d_lon = group_df["lon"].to_numpy()
    d_value = group_df["data_value"].to_numpy()

    grid_data = griddata((d_lon, d_lat), d_value, (grid_lon, grid_lat), method='nearest')
    grid_data_t.append((sample_dt, grid_data))

grid_data_t = sorted(grid_data_t, key=lambda x: x[0])

fig = plt.figure()
ax = plt.axes(projection=ccrs.PlateCarree())
ax.add_feature(cfeature.COASTLINE)
ax.add_feature(cfeature.BORDERS)
ax.add_feature(cfeature.STATES)
ax.add_feature(cfeature.RIVERS)
ax.add_feature(cfeature.LAKES)
# ax.scatter(grid_lon.ravel(), grid_lat.ravel(), s=1)
pcolor_obj = ax.pcolormesh(grid_lon, grid_lat, grid_data_t[0][1], shading='auto')
ax.scatter(station_locations["lon"], station_locations["lat"], s=25, c="orange")
plt.

def animate(i):
    ax.set_title(grid_data_t[i][0])
    pcolor_obj.set_array(grid_data_t[i][1].ravel())


anim = FuncAnimation(
    fig, animate, interval=100, frames=len(grid_data_t))

plt.show()
