import pandas as pd
import numpy as np
from typing import Union

df = pd.read_csv('/md2_python_for_DA/hometasks/final_project/real_estate_data.csv', sep='\t', encoding='utf-8')

def remove_outliers(df: pd.DataFrame, column_name: str) -> pd.DataFrame:
    q1 = df[column_name].quantile(0.25)
    q3 = df[column_name].quantile(0.75)
    iqr = q3 - q1

    df = df[
        df[column_name].between(
            q1 - 1.5 * iqr,
            q3 + 1.5 * iqr,
        )
    ]
    return df


def fill_nulls(df: pd.DataFrame) -> pd.DataFrame:
    df['first_day_exposition'] = pd.to_datetime(df['first_day_exposition']).dt.normalize()
    df['balcony'] = df['balcony'].fillna(0)
    df['ponds_nearest'] = df['ponds_nearest'].fillna(0)
    df['is_apartment'] = df['is_apartment'].astype(bool).fillna(False)
    df['days_exposition'] = df['days_exposition'].fillna(0)
    return df


def make_it_dope(df: pd.DataFrame, var: str, cols: Union[str, list]) -> pd.DataFrame:
    var_avg_name = var + "_avg"
    var_avg_total_value = df[var].mean()

    if var_avg_name in df.columns:
        df.drop(columns=[var_avg_name], inplace=True)

    if var == 'ceiling_height':
        df[var] = df[var].apply(lambda x: x - 10 if 11 < x <= 20 else x)
        df[var] = df[var].apply(lambda x: round(x / 10, 2) if 20 < x <= 100 else x)
        df[var] = df[var].apply(lambda x: round(x / 100, 2) if x > 100 else x)

    df_modify = (
        df
        .groupby(cols)[var]
        .mean()
        .sort_values(ascending=False)
        .reset_index()
        .round(2)
    )
    df_modify.rename(columns={var: var_avg_name}, inplace=True)
    df = df.merge(df_modify, on=cols, how='left')

    df[var] = df[var].fillna(df[var_avg_name])
    df[var] = df[var].fillna(var_avg_total_value)
    df.drop(columns=[var_avg_name], inplace=True)

    return df

df = make_it_dope(df, 'rooms', ['locality_name'])
df = make_it_dope(df, 'ceiling_height', ['locality_name'])
df = make_it_dope(df, 'living_area', ['locality_name', 'rooms', 'ceiling_height'])
df = make_it_dope(df, 'total_area', ['locality_name', 'rooms', 'ceiling_height', 'living_area'])
df = make_it_dope(df, 'kitchen_area', ['locality_name', 'rooms', 'ceiling_height', 'living_area', 'total_area'])





def add_m2_price(df: pd.DataFrame) -> pd.DataFrame:
    df['price_per_m2'] = (df['last_price'] / df['total_area']).round(2)
    return df


def create_bins(series, bins, labels):
    return pd.cut(series, bins=bins, labels=labels, include_lowest=True)


def make_bins(df: pd.DataFrame) -> pd.DataFrame:
    df['cityCenters_nearest'] = df['cityCenters_nearest'].fillna(df['cityCenters_nearest'].max())
    df['parks_nearest'] = df['parks_nearest'].fillna(df['parks_nearest'].max())
    df['floor'] = df['floor'].fillna(df['floor'].min())
    df['living_area'] = df['living_area'].fillna(df['living_area'].mean())

    city_center_bin = {
        'bins': [0, 1000, 3000, 5000, 10000, 20000, float('inf')],
        'labels': ['0-1km', '1-3km', '3-5km', '5-10km', '10-20km', '20+km'],
    }
    park_nearest_bin = {
        'bins': [0, 100, 500, 1000, 2000, float('inf')],
        'labels': ['0-100', '101-500', '501-1000', '1001-2000', '2000+'],
    }
    floor_bin = {
        'bins': [0, 1, 5, 10, 20, float('inf')],
        'labels': ['1', '2-5', '6-10', '11-20', '21+'],
    }
    living_area_bin = {
        'bins': [0, 30, 50, 70, 90, 150, float('inf')],
        'labels': ['0-30', '31-50', '51-70', '71-90', '91-150', '151+'],
    }

    def create_bins(series, bins, labels):
        return pd.cut(series, bins=bins, labels=labels, right=False)

    df['cityCenters_range'] = create_bins(df['cityCenters_nearest'], **city_center_bin)
    df['floor_range'] = create_bins(df['floor'], **floor_bin)
    df['living_area_range'] = create_bins(df['living_area'], **living_area_bin)
    df['parks_nearest_range'] = create_bins(df['parks_nearest'], **park_nearest_bin)

    return df


def get_fair_price(df: pd.DataFrame, var: str, cols: Union[str, list]) -> pd.DataFrame:
    fair_price = 'fair_price'

    if fair_price in df.columns:
        df.drop(columns=[fair_price], inplace=True)

    df_modify = (
        df
        .groupby(cols, observed=True)[var]
        .mean()
        #.round(2)
        .reset_index()
    )
    df_modify.rename(columns={var: fair_price}, inplace=True)
    df_modify[fair_price] = df_modify[fair_price].astype(float)

    df = df.merge(df_modify, on=cols, how='left')
    if var == 'price_per_m2':
        df[fair_price] = df[fair_price] * df['price_per_m2']

    return df


group_by = [
    'locality_name',
    'cityCenters_range',
    'floor_range',
    'living_area_range',
    'parks_nearest_range',
]

df = get_fair_price(df, 'price_per_m2', group_by)