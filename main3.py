import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from typing import Union


class NullFilledDF:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def __getitem__(self, item):
        return self.df[item]

    def __setitem__(self, key, value):
        self.df[key] = value

    def get_df(self) -> pd.DataFrame:
        return self.df

    def fill_nulls(self) -> 'NullFilledDF':
        self.df['first_day_exposition'] = pd.to_datetime(self.df['first_day_exposition']).dt.normalize()
        self.df['balcony'] = self.df['balcony'].fillna(0)
        self.df['ponds_nearest'] = self.df['ponds_nearest'].fillna(0)
        self.df['is_apartment'] = self.df['is_apartment'].astype(bool).fillna(False)
        return self

    def add_useful_cols(self) -> 'NullFilledDF':
        if 'price_per_m2' not in self.df.columns:
            self.df['price_per_m2'] = (self.df['last_price'] / self.df['total_area']).round(2)
        if 'year' not in self.df.columns:
            self.df['year'] = self.df['first_day_exposition'].dt.year
        if 'yyyy_mm' not in self.df.columns:
            self.df['yyyy_mm'] = self.df['first_day_exposition'].dt.strftime('%Y-%m')
        return self


class SuperDF(pd.DataFrame):
    @staticmethod
    def create_bins(series: pd.Series, bins: list, labels: list) -> pd.Series:
        return pd.cut(series, bins=bins, labels=labels, right=False)

    def make_bins(self) -> 'SuperDF':
        self['cityCenters_nearest'] = self['cityCenters_nearest'].fillna(self['cityCenters_nearest'].max())
        self['parks_nearest'] = self['parks_nearest'].fillna(self['parks_nearest'].max())
        self['floor'] = self['floor'].fillna(self['floor'].min())
        self['living_area'] = self['living_area'].fillna(self['living_area'].mean())
        self['total_area'] = self['total_area'].fillna(self['total_area'].mean())
        self['ceiling_height'] = self['ceiling_height'].fillna(self['ceiling_height'].mean())

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
            'bins': [0, 20, 40, 60, 80, 120, float('inf')],
            'labels': ['0-20', '21-40', '41-60', '61-80', '81-120', '121+'],
        }
        total_area_bin = {
            'bins': [0, 30, 50, 70, 90, 150, float('inf')],
            'labels': ['0-30', '31-50', '51-70', '71-90', '91-150', '151+'],
        }
        ceiling_height_bin = {
            'bins': [0, 2.5, 2.8, 3, 3.5, 4.5, float('inf')],
            'labels': ['0-2.5', '2.5-2.8', '2.8-3', '3-3.5', '3.5-4.5', '4.5+'],
        }

        self['cityCenters_range'] = self.create_bins(self['cityCenters_nearest'], **city_center_bin)
        self['parks_nearest_range'] = self.create_bins(self['parks_nearest'], **park_nearest_bin)
        self['floor_range'] = self.create_bins(self['floor'], **floor_bin)
        self['living_area_range'] = self.create_bins(self['living_area'], **living_area_bin)
        self['total_area_range'] = self.create_bins(self['total_area'], **total_area_bin)
        self['ceiling_height_range'] = self.create_bins(self['ceiling_height'], **ceiling_height_bin)

        return self

    def make_it_dope(self, var: str, cols: Union[str, list]) -> 'SuperDF':
        var_avg_name = var + "_avg"
        var_avg_total_value = self[var].mean()

        if var_avg_name in self.columns:
            self.drop(columns=[var_avg_name], inplace=True)

        # it is my ceiling_height logics
        if var == 'ceiling_height':
            self[var] = self[var].apply(lambda x: x - 10 if 11 < x <= 20 else x)
            self[var] = self[var].apply(lambda x: round(x / 10, 2) if 20 < x <= 100 else x)
            self[var] = self[var].apply(lambda x: round(x / 100, 2) if x > 100 else x)

        df_modify = (
            self
            .groupby(cols)[var]
            .mean()
            .sort_values(ascending=False)
            .reset_index()
            .round(2)
        )
        df_modify.rename(columns={var: var_avg_name}, inplace=True)
        df1 = self.merge(df_modify, on=cols, how='left')

        df1[var] = df1[var].fillna(df1[var_avg_name])
        df1[var] = df1[var].fillna(var_avg_total_value)
        df1.drop(columns=[var_avg_name], inplace=True)

        return df1

    def fill_complicated_vars(self, vars: list, cols: Union[str, list]) -> 'SuperDF':
        # run "make_it_dope" for every column from the "vars" to fill it
        prev_var = []
        for var in vars:
            cols += prev_var
            params = {
                'var': var,
                'cols': cols,
            }
            prev_var = [var]
            self.make_it_dope(**params)
        return self

    def get_fair_price(self, var: str, cols: list) -> 'SuperDF':
        fair_price = 'fair_price'

        if fair_price in self.columns:
            self.drop(columns=[fair_price], inplace=True)

        df_modify = (
            self
            .groupby(cols, observed=True)[var]
            .mean()
            .round(2)
            .reset_index()
        )

        if var == 'price_per_m2':
            fair_price = fair_price + '_m2'
            df_modify.rename(columns={var: fair_price}, inplace=True)
        elif var == 'last_price':
            df_modify.rename(columns={var: fair_price}, inplace=True)

        df1 = self.merge(df_modify, on=cols, how='left')

        return df1


def build_plot(df):
    _, ax = plt.subplots(figsize=(12, 5))

    if 'fair_price_m2' in df.columns:
        df_price = df.groupby(['year']).agg({'price_per_m2': 'mean', 'fair_price_m2': 'mean'}).reset_index()
        ax.plot(df_price['year'], df_price['price_per_m2'], label='price_per_m2', color='green')
        ax.plot(df_price['year'], df_price['fair_price_m2'], label='fair_price_m2', color='purple')
        ax.set_title('Fair vs Real prices per m2, $')
    elif 'fair_price' in df.columns:
        df_price = df.groupby(['year']).agg({'last_price': 'mean', 'fair_price': 'mean'}).reset_index()
        ax.plot(df_price['year'], df_price['last_price'], label='price', color='green')
        ax.plot(df_price['year'], df_price['fair_price'], label='fair_price', color='purple')
        ax.set_title('Fair vs Real prices, $')


    ax.legend()
    plt.ylabel('price, $')
    plt.show()


df = pd.read_csv('real_estate_data.csv', encoding='utf-8', sep='\t')
df = NullFilledDF(df)
df = (
    df
    .fill_nulls()
    .add_useful_cols()
    .get_df()
)

PARAMS = {
    'vars': ['ceiling_height', 'rooms', 'living_area', 'total_area', 'days_exposition'],
    'cols': ['locality_name']
}

df = SuperDF(df)

RANGE_COLS = [
    'total_area_range',
    'cityCenters_range',
    'floor_range',
    'living_area_range',
    'parks_nearest_range',
    'ceiling_height_range',
]
fair_price_group_cols = [
    'locality_name',
    'rooms',
] + RANGE_COLS

df = (
    df
    .make_bins()
    .fill_complicated_vars(**PARAMS)
    .get_fair_price('price_per_m2', fair_price_group_cols)
    # .get_fair_price('last_price', fair_price_group_cols)
)

build_plot(df)
