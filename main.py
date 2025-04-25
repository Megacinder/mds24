import numpy as np

A = np.array([[1,2],
              [3,-4]])
B = np.array([[2, 0],
              [0, 1]])
C = np.array([[1, -2, 0],
              [3, 0, -1]])


det_a = np.linalg.det(A)
det_b = np.linalg.det(B)
try:
    det_c = np.linalg.det(C)
except np.linalg.LinAlgError:
    det_c = 'undefined'

print("determinants: ")
print(det_a, det_b, det_c, sep='\n')

eigval_a, eigvec_a = np.linalg.eig(A)
eigval_b, eigvec_b = np.linalg.eig(B)
try:
    eigval_c, eigvec_c = np.linalg.eig(C)
except np.linalg.LinAlgError:
    eigval_c, eigvec_c = 'undefined', 'undefined'

print("eigevals: ")
print(eigval_a, eigval_b, eigval_c, sep='\n')

print("eigevecs: ")
print(eigvec_a, eigvec_b, eigvec_c, sep='\n')


inv_a = np.linalg.inv(A)
inv_b = np.linalg.inv(B)
try:
    inv_c = np.linalg.inv(C)
except np.linalg.LinAlgError:
    inv_c = 'undefined'


print("inverse matrices: ")
print(inv_a, inv_b, inv_c, sep='\n')


