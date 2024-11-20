from typing import List
from timeit import timeit


def n1_deadline(n: int) -> list:
    list1 = []
    for i in range(n, 0, -1):
        list1 += [f"Hours to deadline: {i}"]

    list1 += ["Time to hand in your work!"]
    return list1


def run_n1():
    a = int(input())
    b = n1_deadline(a)
    print(*b, sep='\n')


def n2_delegation_from_japan(li: list) -> list:
    list1 = [i + '-san' for i in li]
    return list1


def run_n2():
    a = int(input())
    li = []
    for i in range(a):
        li += [input()]

    b = n2_delegation_from_japan(li)
    print(*b, sep='\n')


def n3_the_word_game():
    i = 0
    prev = ''
    while True:
        cur = input()
        if prev and (cur[0] != prev[-1] or len(cur) - 1 != len(prev)):
            break
        prev = cur
        i += 1
    print(i)


def n4_excellence_scholarship_2(li: List[int]) -> int:
    s = 20000
    a = 0
    for i in li:
        if i >= s:
            a += 1
    return a


def run_n4():
    li = []
    while True:
        a = int(input())
        if a < 0:
            break
        li += [a]
    print(n4_excellence_scholarship_2(li))


def n5_expenses_on_snacks(li: list) -> int:
    a = max(li) if li else 0
    b = 0
    for i in li:
        if i >= a:
            b += 1
    return b


def run_n5():
    li = []
    while True:
        a = int(input())
        if a < 0:
            break
        li += [a]
    print(n5_expenses_on_snacks(li))


def run_n5_alt1():
    a = [1, 2, 3, 4, 5, 6, 7, 8, 8, ]
    return n5_expenses_on_snacks(a)


def n5_expenses_on_snacks2(li: list) -> int:
    a = 0
    b = 0

    for i in li:
        if i > a:
            a = i
            b = 1
        else:
            b += 1
    return b


def run_n5_alt2():
    a = [1, 2, 3, 4, 5, 6, 7, 8, 8, ]
    return n5_expenses_on_snacks2(a)


def n6_word_search(a: str, b: str, n: str) -> str:
    li = f'{a} {b}'.split()
    n = int(n)
    if n <= 0 or n > len(li):
        return "There is no such word"
    return li[n - 1]


def run_n6():
    a = input()
    b = input()
    n = input()
    print(n6_word_search(a, b, n))


def n7_password(a: str, b: str) -> list:
    li = []
    b = b.split()
    for i in b:
        if a in i:
            li += ["True"]
        else:
            li += ["False"]
    return li


def run_n7():
    a = input()
    b = input()
    print(*n7_password(a, b), sep='\n')


def n8_game_cities_of_the_world(input_str1: str, input_str2: str) -> list:
    o_li = []
    input_str1 = input_str1.split(', ')

    for i in input_str1:
        if i[0] == input_str2:
            o_li += [i]
    return o_li


def run_n8():
    input_str1 = input()
    input_str2 = input()
    print(*n8_game_cities_of_the_world(input_str1, input_str2), sep='\n')


def n9_house_chores_2(input_str1: str) -> list:
    o_li1 = []
    o_li2 = []
    input_str1 = input_str1.split(', ')

    for ix, i in enumerate(input_str1):
        if ix % 2 == 0:
            o_li2 += [i]
        else:
            o_li1 += [i]
    return [", ".join(o_li1), ", ".join(o_li2)]


def run_n9():
    input_str1 = input()
    print(*n9_house_chores_2(input_str1), sep='\n')


def n10_athletes(input_str1: str, input_str2: str) -> list:
    o_li = []
    input_str1 = input_str1.split()
    input_str2 = input_str2.split()

    for j in input_str2:
        for ix, i in enumerate(input_str1):
            if ix == int(j) - 1:
                o_li += [f"{j} place: {i}"]

        if int(j) > len(input_str1):
            o_li += [f"{j} place: No athlete"]

    return o_li


def run_n10():
    input_str1 = input()
    input_str2 = input()
    print(*n10_athletes(input_str1, input_str2), sep='\n')


if __name__ == "__main__":
    # exec_time = timeit(run_n5_alt1)
    run_n10()
