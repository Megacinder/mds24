import numpy as np



n = 3000
A = np.random.randn(n, n)
B = np.random.randn(n, n)

C = np.zeros((n, n))

C = A @ B
print(C)
