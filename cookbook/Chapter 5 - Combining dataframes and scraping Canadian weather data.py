# %%
import pandas as pd
import polars as pl
import matplotlib.pyplot as plt
import numpy as np

plt.style.use("ggplot")
plt.rcParams["figure.figsize"] = (15, 3)
plt.rcParams["font.family"] = "sans-serif"

# %%
# By the end of this chapter, we're going to have downloaded all of Canada's weather data for 2012, and saved it to a CSV. We'll do this by downloading it one month at a time, and then combining all the months together.
# Here's the temperature every hour for 2012!

weather_2012_final = pd.read_csv("./data/weather_2012.csv", index_col="date_time")
weather_2012_final["temperature_c"].plot(figsize=(15, 6))
plt.show()

# TODO: rewrite using Polars

pl_weather_2012 = pl.read_csv(
    "./data/weather_2012.csv"
    ).with_columns(
        pl.col("date_time").str.strptime(pl.Datetime).alias("date_time")
    )

plt.figure(figsize=(15, 6))
plt.plot(pl_weather_2012["date_time"].to_list(),
         pl_weather_2012["temperature_c"].to_list())
plt.show()

# %%
# Okay, let's start from the beginning.
# We're going to get the data for March 2012, and clean it up
# You can directly download a csv with a URL using Pandas!
# Note, the URL the repo provides is faulty but kindly, someone submitted a PR fixing it. Have a look
# here: https://github.com/jvns/pandas-cookbook/pull/74 and click on "Files changed" and then fix the url.


# This URL has to be fixed first!
url_template = "http://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID=5415&Year={year}&Month={month}&timeframe=1&submit=Download+Data"

year = 2012
month = 3
url_march = url_template.format(month=3, year=2012)
weather_mar2012 = pd.read_csv(
    url_march,
    index_col="Date/Time (LST)",
    parse_dates=True,
    encoding="latin1",
    header=0,
)
weather_mar2012.head(5)

# TODO: rewrite using Polars. Yes, Polars can handle URLs similarly.

url_template_fixed = "http://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID=5415&Year={year}&Month={month}&timeframe=1&submit=Download+Data"

year = 2012
month = 3
url_march = url_template_fixed.format(month=month, year=year)

pl_weather_mar2012 = pl.read_csv(
    url_march,
    encoding="latin1"
    ).with_columns(
        pl.col("Date/Time (LST)").str.strptime(pl.Datetime, strict=False)
        )

pl_weather_mar2012.head(5)

# %%
# Let's clean up the data a bit.
# You'll notice in the summary above that there are a few columns which are are either entirely empty or only have a few values in them. Let's get rid of all of those with `dropna`.
# The argument `axis=1` to `dropna` means "drop columns", not rows", and `how='any'` means "drop the column if any value is null".
weather_mar2012 = weather_mar2012.dropna(axis=1, how="any")
weather_mar2012[:5]

# This is much better now -- we only have columns with real data.

# TODO: rewrite using Polars

null_alternatives = ["", "NA", "N/A", "M", "E"]

pl_weather_mar2012 = pl.read_csv(
        url_march,
        encoding="latin1",
        null_values=null_alternatives
    ).with_columns(
        pl.col("Date/Time (LST)").str.strptime(pl.Datetime)
    )

null_counts = pl_weather_mar2012.null_count()
columns_no_nulls = [c for c, n in zip(pl_weather_mar2012.columns, null_counts.row(0)) if n == 0]
pl_weather_mar2012 = pl_weather_mar2012.select(columns_no_nulls)

pl_weather_mar2012.head(5)

# %%
# Let's get rid of columns that we do not need.
# For example, the year, month, day, time columns are redundant (we have Date/Time (LST) column).
# Let's get rid of those. The `axis=1` argument means "Drop columns", like before. The default for operations like `dropna` and `drop` is always to operate on rows.
weather_mar2012 = weather_mar2012.drop(["Year", "Month", "Day", "Time (LST)"], axis=1)
weather_mar2012[:5]

# TODO: redo this using polars

columns_to_drop = [c for c in ["Year", "Month", "Day", "Time (LST)"] if c in pl_weather_mar2012.columns]
pl_weather_mar2012 = pl_weather_mar2012.drop(columns_to_drop)

pl_weather_mar2012.head(5)

# %%
# When you look at the data frame, you see that some column names have some weird characters in them.
# Let's clean this up, too.
# Let's print the column names first:
weather_mar2012.columns

# And now rename the columns to make it easier to work with
weather_mar2012.columns = weather_mar2012.columns.str.replace(
    'ï»¿"', ""
)  # Remove the weird characters at the beginning
weather_mar2012.columns = weather_mar2012.columns.str.replace(
    "Â", ""
)  # Remove the weird characters at the

# TODO: rewrite using Polars

cleaned_columns = [c.replace('ï»¿"', "").replace("Â", "") for c in pl_weather_mar2012.columns]
pl_weather_mar2012 = pl_weather_mar2012.rename(dict(zip(pl_weather_mar2012.columns, cleaned_columns)))

pl_weather_mar2012.head(5)

# %%
# Optionally, you can also rename columns more manually for specific cases:
weather_mar2012 = weather_mar2012.rename(
    columns={
        'Longitude (x)"': "Longitude",
        "Latitude (y)": "Latitude",
        "Station Name": "Station_Name",
        "Climate ID": "Climate_ID",
        "Temp (°C)": "Temperature_C",
        "Dew Point Temp (Â°C)": "Dew_Point_Temp_C",
        "Rel Hum (%)": "Relative_Humidity",
        "Wind Spd (km/h)": "Wind_Speed_kmh",
        "Visibility (km)": "Visibility_km",
        "Stn Press (kPa)": "Station_Pressure_kPa",
        "Weather": "Weather",
    }
)
weather_mar2012.index.name = "date_time"

# Check the new column names
print(weather_mar2012.columns)

# Some people also prefer lower case column names.
weather_mar2012.columns = weather_mar2012.columns.str.lower()
print(weather_mar2012.columns)

# TODO: redo this using polars

rename_map = {
    'Longitude (x)"': "Longitude",
    "Longitude (x)": "Longitude",
    "Latitude (y)": "Latitude",
    "Station Name": "Station_Name",
    "Climate ID": "Climate_ID",
    "Temp (°C)": "Temperature_C",
    "Dew Point Temp (°C)": "Dew_Point_Temp_C",
    "Dew Point Temp (Â°C)": "Dew_Point_Temp_C",
    "Rel Hum (%)": "Relative_Humidity",
    "Wind Spd (km/h)": "Wind_Speed_kmh",
    "Visibility (km)": "Visibility_km",
    "Stn Press (kPa)": "Station_Pressure_kPa",
    "Weather": "Weather"
}
pl_weather_mar2012 = pl_weather_mar2012.rename({k: v for k, v in rename_map.items() if k in pl_weather_mar2012.columns})

lower_cols = {c: c.lower() for c in pl_weather_mar2012.columns}
pl_weather_mar2012 = pl_weather_mar2012.rename(lower_cols)

if "date/time (lst)" in pl_weather_mar2012.columns:
    pl_weather_mar2012 = pl_weather_mar2012.rename({"date/time (lst)": "date_time"})

pl_weather_mar2012.head(5)

# %%
# Notice how it goes up to 25° C in the middle there? That was a big deal. It was March, and people were wearing shorts outside.
weather_mar2012["temperature_c"].plot(figsize=(15, 5))
plt.show()

# TODO: redo this using polars

plt.figure(figsize=(15, 5))
plt.plot(pl_weather_mar2012["date_time"].to_list(),
         pl_weather_mar2012["temperature_c"].to_list())
plt.show()

# %%
# This one's just for fun -- we've already done this before, using groupby and aggregate! We will learn whether or not it gets colder at night. Well, obviously. But let's do it anyway.
temperatures = weather_mar2012[["temperature_c"]].copy()
print(temperatures.head)
temperatures.loc[:, "Hour"] = weather_mar2012.index.hour
temperatures.groupby("Hour").aggregate(np.median).plot()
plt.show()

# So it looks like the time with the highest median temperature is 2pm. Neat.

# TODO: redo this using polars

pl_temps_by_hour = (
    pl_weather_mar2012
    .with_columns(pl.col("date_time").dt.hour().alias("hour"))
    .group_by("hour")
    .agg(pl.col("temperature_c").median().alias("median_temp_c"))
    .sort("hour")
)

plt.figure()
plt.plot(pl_temps_by_hour["hour"].to_list(),
         pl_temps_by_hour["median_temp_c"].to_list())
plt.show()

# %%
# Okay, so what if we want the data for the whole year? Ideally the API would just let us download that, but I couldn't figure out a way to do that.
# First, let's put our work from above into a function that gets the weather for a given month.


def clean_data(data):
    data = data.dropna(axis=1, how="any")
    data = data.drop(["Year", "Month", "Day", "Time (LST)"], axis=1)
    data.columns = data.columns.str.replace('ï»¿"', "")
    data.columns = data.columns.str.replace("Â", "")
    data = data.rename(
        columns={
            "Longitude (x)": "Longitude",
            "Latitude (y)": "Latitude",
            "Station Name": "Station_Name",
            "Climate ID": "Climate_ID",
            "Temp (°C)": "Temperature_C",
            "Dew Point Temp (°C)": "Dew_Point_Temp_C",
            "Rel Hum (%)": "Relative_Humidity",
            "Wind Spd (km/h)": "Wind_Speed_kmh",
            "Visibility (km)": "Visibility_km",
            "Stn Press (kPa)": "Station_Pressure_kPa",
            "Weather": "Weather",
        }
    )
    data.columns = data.columns.str.lower()
    data.index.name = "date_time"
    return data


def download_weather_month(year, month):
    url_template = "http://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID=5415&Year={year}&Month={month}&timeframe=1&submit=Download+Data"
    url = url_template.format(year=year, month=month)
    weather_data = pd.read_csv(
        url, index_col="Date/Time (LST)", parse_dates=True, header=0
    )
    weather_data_clean = clean_data(weather_data)
    return weather_data_clean


# TODO: redefine these functions using polars and your code above

def clean_data_pl(df: pl.DataFrame) -> pl.DataFrame:
    nc = df.null_count()
    keep_columns = [c for c, n in zip(df.columns, nc.row(0)) if n == 0]
    df = df.select(keep_columns)

    cleaned = [c.replace('ï»¿"', "").replace("Â", "") for c in df.columns]
    df = df.rename(dict(zip(df.columns, cleaned)))

    drop_cols = [c for c in ["Year", "Month", "Day", "Time (LST)"] if c in df.columns]
    df = df.drop(drop_cols)

    rename_map = {
        'Longitude (x)"': "Longitude",
        "Longitude (x)": "Longitude",
        "Latitude (y)": "Latitude",
        "Station Name": "Station_Name",
        "Climate ID": "Climate_ID",
        "Temp (°C)": "Temperature_C",
        "Dew Point Temp (°C)": "Dew_Point_Temp_C",
        "Dew Point Temp (Â°C)": "Dew_Point_Temp_C",
        "Rel Hum (%)": "Relative_Humidity",
        "Wind Spd (km/h)": "Wind_Speed_kmh",
        "Visibility (km)": "Visibility_km",
        "Stn Press (kPa)": "Station_Pressure_kPa",
        "Weather": "Weather",
    }
    df = df.rename({k: v for k, v in rename_map.items() if k in df.columns})
    df = df.rename({c: c.lower() for c in df.columns})

    if "date/time (lst)" in df.columns:
        df = df.rename({"date/time (lst)": "date_time"})
    df = df.with_columns(pl.col("date_time").cast(pl.Datetime, strict=False))

    numeric_like = [
        "temperature_c",
        "dew_point_temp_c",
        "relative_humidity",
        "wind_speed_kmh",
        "visibility_km",
        "station_pressure_kpa",
    ]
    df = df.with_columns(
        [
            pl.col(c).cast(pl.Float64, strict=False)
            for c in numeric_like
            if c in df.columns
        ]
    )

    return df


def download_weather_month_pl(year: int, month: int) -> pl.DataFrame:
    url_template_fixed = "http://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID=5415&Year={year}&Month={month}&timeframe=1&submit=Download+Data"
    url = url_template_fixed.format(year=year, month=month)

    null_alternatives = ["", "NA", "N/A", "M", "E"]

    df = pl.read_csv(url, encoding="latin1", null_values=null_alternatives)

    if "Date/Time (LST)" in df.columns:
        df = df.with_columns(pl.col("Date/Time (LST)").str.strptime(pl.Datetime, strict=False))

    return clean_data_pl(df)

# %%
download_weather_month(2012, 1)[:5]
# %%
# Now, let's use a list comprehension to download all our data and then just concatenate these data frames
# This might take a while
data_by_month = [download_weather_month(2012, i) for i in range(1, 13)]
weather_2012 = pd.concat(data_by_month)
weather_2012.head()

# TODO: do the same with polars

download_weather_month_pl(2012, 1).head(5)

pl_data_by_month = [download_weather_month_pl(2012, m) for m in range(1, 13)]
pl_weather_2012_full = pl.concat(pl_data_by_month).sort("date_time")

pl_weather_2012_full.head(5)

# %%
# Now, let's save the data.
weather_2012.to_csv("../data/weather_2012.csv")

# TODO: use polars to save the data.

pl_weather_2012_full.write_csv("./data/pl_weather_2012_full.csv")
