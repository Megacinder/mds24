class NullFilledDF:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def __getitem__(self, item):
        return self.df[item]

    def fill_nulls(self) -> 'NullFilledDF':
        self.df['first_day_exposition'] = pd.to_datetime(self.df['first_day_exposition']).dt.normalize()
        self.df['balcony'] = self.df['balcony'].fillna(0)
        self.df['ponds_nearest'] = self.df['ponds_nearest'].fillna(0)
        self.df['is_apartment'] = self.df['is_apartment'].astype(bool).fillna(False)
        return self

    def add_m2_price(self) -> 'NullFilledDF':
        self.df['price_per_m2'] = (self.df['last_price'] / self.df['total_area']).round(2)
        return self

    def get_df(self) -> pd.DataFrame:
        return self.df

df = NullFilledDF(df)
df = df.add_m2_price()