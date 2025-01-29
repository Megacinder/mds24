from pandas import read_csv

a = read_csv('iris.csv', header='infer', delimiter=',')
a.set_index('variety', inplace=True)
print(a.loc['Setosa'])