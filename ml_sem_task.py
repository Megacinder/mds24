# from pandas import DataFrame
# from sklearn.model_selection import train_test_split
# from sklearn.neighbors import KNeighborsClassifier
#
# DATA = {
#     'name': ['toyota', 'lada', 'bmw', 'ikarus', 'toyota', 'lada', 'bmw', 'ikarus'],
#     'age': [25, 30, 35, 28, 25, 30, 35, 28],
#     'price': [100, 200, 300, 50, 100, 200, 300, 50],
#     'salary': [1, 2, 3, 5, 10, 2, 3, 5],
# }
#
# df = DataFrame(DATA)
#
# FEATURE = "salary"
# X = df.drop(columns=[FEATURE])
# y = df[FEATURE]
# X_train, X_valid, y_train, y_valid = train_test_split(X, y, test_size=0.25, random_state=13)
#
# print(X_valid)

# knn = KNeighborsClassifier(n_neighbors=3, metric='cityblock')
# knn.fit(X_train, y_train)
# y_pred = knn.predict(X_valid)

# print(y_pred)


import matplotlib.pyplot as plt
import numpy as np

x = np.arange(-10, 10, 0.5)
y = np.sin(x)

print(plt.plot(x))