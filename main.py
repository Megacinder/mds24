import numpy as np

from decimal import Decimal, getcontext, ROUND_UP, ROUND_HALF_UP

import math


# defining round_half_up

def round2(number: int | float, precision: int = 0) -> float:
    from math import floor
    multiplier = 10 ** precision
    return floor(number * multiplier + 0.5) / multiplier


# def round2(number: float, precision: int = 0) -> float:
#     from decimal import Decimal
#     a = Decimal(number)
#     a = a.quantize(Decimal(f"1.{'0' * precision}"))
#     return a

b = round2(6.45, 1)

print(b)

# a = 6.50001
# def round2()
# b = np.where(a - np.floor(a) < 0.5, np.floor(a), np.ceil(a))
# print(b)

# rnd = lambda v, p=0: round(v * (10 ** p)) / (10 ** p)

# a = 6.5
# b = Decimal(a).to_integral_value(rounding=ROUND_HALF_UP, )
# print(b)

# getcontext().rounding = ROUND_UP
# # print(getcontext())
# b = round(Decimal(6.5))
# print(b)