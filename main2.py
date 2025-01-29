import pandas as pd

a = {
    'a': [1, 2, 3],
    'b': [4, 5, 6],
    'c': [7, 8, 9],
}

df = pd.DataFrame(a)

print(df.sort_values(['a', 'b'], ascending=[True, False]))

