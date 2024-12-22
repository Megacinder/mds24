class NullFilledDF:
    def __init__(self, df: pd.DataFrame):
        if not isinstance(df, pd.DataFrame):
            raise ValueError("df должен быть объектом pandas.DataFrame")
        self.df = df

    def __getitem__(self, item):
        return self.df[item]

    def __setitem__(self, key, value):
        self.df[key] = value

    def get_df(self) -> pd.DataFrame:
        return self.df

    def fill_nulls(self) -> 'NullFilledDF':
        self.df['first_day_exposition'] = pd.to_datetime(self.df['first_day_exposition'], errors='coerce').dt.normalize()
        self.df['balcony'] = self.df['balcony'].fillna(0)
        self.df['ponds_nearest'] = self.df['ponds_nearest'].fillna(0)
        self.df['is_apartment'] = self.df['is_apartment'].fillna(False).astype(bool)
        return self

    def add_useful_cols(self) -> 'NullFilledDF':
        if 'price_per_m2' not in self.df.columns:
            self.df['price_per_m2'] = (self.df['last_price'] / self.df['total_area']).round(2)
        if 'year' not in self.df.columns:
            self.df['year'] = self.df['first_day_exposition'].dt.year
        if 'yyyy_mm' not in self.df.columns:
            self.df['yyyy_mm'] = self.df['first_day_exposition'].dt.strftime('%Y-%m')
        return self
