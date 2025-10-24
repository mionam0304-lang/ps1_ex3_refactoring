# %%
import pandas as pd
import polars as pl
import matplotlib.pyplot as plt
import numpy as np

plt.style.use("ggplot")
plt.rcParams["figure.figsize"] = (15, 3)
plt.rcParams["font.family"] = "sans-serif"


# %%
# We saw earlier that pandas is really good at dealing with dates. It is also amazing with strings! We're going to go back to our weather data from Chapter 5, here.
weather_2012 = pd.read_csv(
    "./data/weather_2012.csv", parse_dates=True, index_col="date_time"
)
weather_2012[:5]

# TODO: load the data using polars and call the data frame pl_wather_2012

pl_weather_2012 = pl.read_csv(
    "./data/pl_weather_2012_full.csv"
    ).with_columns(
        pl.col("date_time").str.strptime(pl.Datetime)
    )

pl_weather_2012.head(5)

# %%
# You'll see that the 'Weather' column has a text description of the weather that was going on each hour. We'll assume it's snowing if the text description contains "Snow".
# Pandas provides vectorized string functions, to make it easy to operate on columns containing text. There are some great examples: "http://pandas.pydata.org/pandas-docs/stable/basics.html#vectorized-string-methods" in the documentation.
weather_description = weather_2012["weather"]
is_snowing = weather_description.str.contains("Snow")

# Let's plot when it snowed and when it did not:
is_snowing = is_snowing.astype(float)
is_snowing.plot()
plt.show()

# TODO: do the same with polars

pl_is_snowing = pl_weather_2012.select(
    "date_time",
    pl.when(pl.col("weather").str.contains("Snow")).then(1.0).otherwise(0.0).alias("is_snowing")
)

plt.figure(figsize=(15, 5))
plt.plot(pl_is_snowing["date_time"].to_list(), pl_is_snowing["is_snowing"].to_list())
plt.show()

# %%
# If we wanted the median temperature each month, we could use the `resample()` method like this:
weather_2012["temperature_c"].resample("M").apply(np.median).plot(kind="bar")
plt.show()

# Unsurprisingly, July and August are the warmest.

# TODO: and now in Polars

pl_weather_2012_monthly_temp = pl_weather_2012.with_columns(
        pl.col("date_time").dt.year().alias("year"),
        pl.col("date_time").dt.month().alias("month")
        ).group_by(["year", "month"]).agg(
            pl.col("temperature_c").median().alias("median_temp_c")
            ).sort(["year", "month"])

labels = [f"{m:02d}" for m in pl_weather_2012_monthly_temp["month"].to_list()]
vals   = pl_weather_2012_monthly_temp["median_temp_c"].to_list()

plt.figure(figsize=(15, 5))
xs = range(len(labels))
plt.bar(xs, vals)
plt.xticks(xs, labels, rotation=0)
plt.show()

# %%
# So we can think of snowiness as being a bunch of 1s and 0s instead of `True`s and `False`s:
is_snowing.astype(float)[:10]

# and then use `resample` to find the percentage of time it was snowing each month
is_snowing.astype(float).resample("M").apply(np.mean).plot(kind="bar")
plt.show()

# So now we know! In 2012, December was the snowiest month. Also, this graph suggests something that I feel -- it starts snowing pretty abruptly in November, and then tapers off slowly and takes a long time to stop, with the last snow usually being in April or May.

# TODO: please do the same in Polars

pl_monthly_snowiness = pl_weather_2012.with_columns(
        pl.when(
            pl.col("weather").str.contains("Snow")
            ).then(1.0).otherwise(0.0).alias("is_snowing"),
        pl.col("date_time").dt.year().alias("year"),
        pl.col("date_time").dt.month().alias("month")
    ).group_by(["year", "month"]).agg(
        pl.col("is_snowing").mean().alias("snow_frac")
        ).sort(["year", "month"])

labels = [f"{m:02d}" for m in pl_monthly_snowiness["month"].to_list()]
vals_pct = [v for v in pl_monthly_snowiness["snow_frac"].to_list()]

plt.figure(figsize=(15, 5))
xs = range(len(labels))
plt.bar(xs, vals_pct)
plt.xticks(xs, labels, rotation=0)
plt.show()
