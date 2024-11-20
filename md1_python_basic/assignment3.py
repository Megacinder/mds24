from typing import List
from timeit import timeit


def n1_email(li: list) -> int:
    a = 0
    for i in li:
        a += i.count('@')
    return a


def run_n1():
    a = input()
    a = int(a)
    li1 = []
    for i in range(a):
        s = input()
        li1 += s

    print(n1_email(li1))


def n2_exam_results(s1: str, s2: str) -> tuple:
    s1 = s1.split()
    s2 = s2.split()
    cutoff = 4
    passed = []
    failed = []
    for a, b in zip(s1, s2):
        if int(b) > cutoff:
            passed += [a]
        else:
            failed += [a]
    return failed, passed


def run_n2():
    s1 = input()
    s2 = input()

    f1, p1 = n2_exam_results(s1, s2)

    print(*f1)
    print(*p1)


def n3_website_registration(s: str) -> str:
    if (
        any([i.isupper() for i in s])
        and any([i.islower() for i in s])
        and any([i.isdigit() for i in s])
        and s.isalnum()
    ):
        return "Password is valid"
    return "Password is not valid"


def n3_website_registration2(s: str) -> str:
    valid = 'Password is valid'
    not_valid = 'Password is not valid'

    a = 0

    for i in s:
        if not i.isalnum():
            return not_valid

        if i.isupper() and a not in (1, 3, 7):
            a += 1
            continue
        elif i.islower() and a not in (2, 3, 6):
            a += 2
            continue
        elif i.isdigit() and a not in (4, 6, 7):
            a += 4
            continue

    print(a)
    if a == 7:
        return valid

    return not_valid


def run_n3():
    s1 = input()
    print(n3_website_registration(s1))


def n4_expenditures_on_lunches(s: str) -> float:
    s = s.split(', ')[:5]
    a = float(s[0])
    for i in s:
        i = float(i)
        if i < a:
            a = i
    return a


def run_n4():
    s1 = input()
    print(n4_expenditures_on_lunches(s1))


def n5_encrypted_message(s: str) -> str:
    s = s.split()
    li1 = []
    for i in s:
        li1 += [i[2:-2]]

    return ' '.join(li1)


def run_n5():
    s1 = input()
    print(n5_encrypted_message(s1))


def n6_magic_wands(s: list, y: str) -> str:
    li1 = []

    for i in s:
        a, b, c = i.split(', ')
        if b == y:
            li1 += [f"{a} (material - {c})"]

    if li1:
        o = f"In {y} the following people bought a wand: {', '.join(li1)}."
    else:
        o = "No one was buying magic wands this year."

    return o


def run_n6():
    s1 = []
    while True:
        s = input()
        if s == "End":
            y = input()
            break
        else:
            s1 += [s]

    print(n6_magic_wands(s1, y))


def n7_club_enrolment(li1: list) -> int:
    di1 = {}
    for i in li1:
        s = i.split(', ')
        for j in s:
            if j not in di1:
                di1[j] = 1
            else:
                di1[j] += 1
    # return len([i for i in di1.values() if i == len(li1)])
    return len(list(filter(lambda x: x == len(li1), di1.values())))


def run_n7():
    li1 = []
    for i in range(3):
        li1 += [input()]
    print(n7_club_enrolment(li1))


def n8_cakes_on_instagram(li1: list, li2: list) -> int:
    return len(set(li1) - set(li2))


def run_n8():
    li1 = []
    while True:
        s = input()
        if s == "END":
            break
        li1 += [s]

    li2 = []
    while True:
        s = input()
        if s == "END":
            break
        li2 += [s]

    print(n8_cakes_on_instagram(li1, li2))


def n9_kanji(li1: list, w: str) -> str:
    di1 = {}
    for i in li1:
        a, b = i.split()
        di1[a] = b

    return "There is no such word" if w not in di1.keys() else di1[w]


def run_n9():
    li1 = []
    w = ''
    while True:
        s = input()
        if s == "END":
            w = input()
            break
        li1 += [s]
    print(n9_kanji(li1, w))


def n10_searching_for_a_book(di1: dict, b: str) -> str:
    li1 = []
    for k, v in di1.items():
        if b in k or b in v:
            li1 += [f"{v}: {k}"]
    return "\n".join(li1)


def run_n10():
    books = {
        'The Silmarillion': 'John Tolkien',
         'Harry Potter and the Goblet of Fire': 'Joanne Rowling',
         'The Indomitable Planet': 'Harry Harrison',
         'The Man Without a Face': 'Alfred Bester',
         'Alice in Wonderland': 'Lewis Carroll',
         'A Space Odyssey': 'Arthur C. Clarke',
         'Dune': 'Frank Herbert',
         'Twenty Thousand Leagues Under the Sea': 'Jules Verne',
         'A Connecticut Yankee in King Arthurs Court': 'Mark Twain',
         'American Gods': 'Neil Gaiman',
         'The Chronicles of Amber': 'Roger Gélasny',
         'The Chronicles of Narnia': 'Clive S. Lewis',
         'War of the Worlds': 'Herbert Wells',
    }
    s = input()
    print(n10_searching_for_a_book(books, s))


if __name__ == "__main__":
    run_n10()
