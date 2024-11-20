from typing import List
from timeit import timeit


def n1_jolly_movers(s: str) -> str:
    li = s.split()
    li = list(map(float, li))
    li = sorted(li, reverse=True)
    li = list(map(str, li))
    return ' '.join(li)


def run_n1():
    s = input()
    print(n1_jolly_movers(s))


def n2_number_of_visitors_to_the_cafe(li: list, hh: int) -> list:
    o_li = []
    for i in li:
        a = i.split(', ')
        o_li.append(a[hh])
    return o_li


def run_n2():
    a = input()
    a = int(a)
    li = []
    for i in range(a):
        a = input()
        li.append(a)

    hh = input()
    hh = int(hh)
    print(*n2_number_of_visitors_to_the_cafe(li, hh), sep='\n')


def n3_music_library(li: list, s: str) -> str:
    di = {}
    for i in li:
        k, v = i.split(': ')
        if k == s:
            if k not in di:
                di[k] = [v]
            else:
                di[k].append(v)

    a = f"{s} ({len(di[s])}): {', '.join(sorted(di[s]))}."

    return a


def run_n3():
    li = []
    while True:
        s = input()
        if ':' not in s:
            break
        li.append(s)
    print(n3_music_library(li, s))


def n4_grammys_award(li: list) -> str:
    di = {}
    for i in li:
        if i not in di:
            di[i] = 1
        else:
            di[i] += 1

    di = {k: v for k, v in sorted(di.items(), key=lambda i: (i[1], i[0]))}

    o_li = []
    for k, v in di.items():
        o_li.append(f"{k}: {v}")

    o_li = [o_li[-1], o_li[0]]

    return '\n'.join(o_li)


def run_n4():
    a = input()
    a = int(a)
    li = []
    for i in range(a):
        s = input()
        li.append(s)
    print(n4_grammys_award(li))


# n5
def get_calories(a: int) -> float:
    return a / 4184


# n6
def filter_accounts(di: dict, min_followers: int, max_followers: int) -> list:
    di1 = {k: v for k, v in di.items() if min_followers <= v <= max_followers}
    o_li = sorted(di1.keys())
    return o_li


def n7_a_telegram_from_matroskin_2(s: str) -> str:
    from string import punctuation
    o = [i.upper() for i in s if i not in punctuation]
    return ''.join(o)


def run_n7():
    s = input()
    print(n7_a_telegram_from_matroskin_2(s))


def n8_round_rug(s: str) -> float:
    from math import pi, sqrt
    d = int(s)
    return pi * sqrt(d)


def run_n8():
    s = input()
    print(n8_round_rug(s))


if __name__ == "__main__":
    run_n8()
