import numpy as np
from pandas import DataFrame
from sklearn.model_selection import train_test_split
from sklearn.neighbors import KNeighborsClassifier, KNeighborsRegressor

DATA = {
    # 'make': ['toyota', 'lada', 'bmw', 'ikarus', 'toyota', 'lada', 'bmw', 'ikarus'],
    'age': [25, 30, 35, 28, 25, 30, 35, 28],
    'price': [100, 200, 300, 50, 100, 200, 300, 50],
    'cylinder_no': [1, 2, 3, 5, 10, 2, 3, 5],
}
df = DataFrame(DATA)

X = df.drop(columns=["price"])
y = df["price"]
X_train, X_valid, y_train, y_valid = train_test_split(X, y, test_size=0.2, random_state=13)

# print(X_valid)

knn = KNeighborsRegressor(n_neighbors=3, metric='cityblock')
knn.fit(X_train, y_train)
y_pred = knn.predict(X_valid)

# print(y_pred)

MSE = lambda y1, y2: ((y1 - y2) ** 2).mean()
MAE = lambda y1, y2: (np.absolute(y1 - y2)).mean()
MAX_ERROR = lambda y1, y2: (np.absolute(y1 - y2)).max()
print(MSE(y_pred, y_valid))
print(MAE(y_pred, y_valid))
print(MAX_ERROR(y_pred, y_valid))