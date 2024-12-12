FROM_STORE = 1
TO_STORE = 100

STEP_PLUS = 7
STEP_MINUS = 9

STORES = [i for i in range(FROM_STORE, TO_STORE + 1)]

b = FROM_STORE

di1 = dict()
li1 = []



def reachable_floors(start, max_floor) -> list:
    reachable = []
    to_visit = [start]
    a = 0
    while to_visit:
        current = to_visit.pop()
        if a != 0:
            aa = " +7" if a == 7 else " -9"
        else:
            aa = ""
        print(f"Current floor: {current}  {aa}{to_visit}")
        a = 0
        if current not in reachable:
            reachable.append(current)
            if current + 7 <= max_floor:
                to_visit.append(current + 7)
                a = 7
            if current - 9 >= 1:
                to_visit.append(current - 9)
                a = -9
    return reachable

fin = reachable_floors(FROM_STORE, TO_STORE)
print(fin)
print(sorted(fin), len(fin))





def aaa():
    for i in STORES:  # [a for a in STORES if a <= 2]:
        b = i
        while True:
            if b < FROM_STORE or b > TO_STORE or len(li1) == TO_STORE:
                break

            # if b in li1 and b - STEP_MINUS in li1 and b + STEP_PLUS in li1:
            #     break

            li1.append(b)

            if b - STEP_MINUS >= FROM_STORE and b - STEP_MINUS not in li1:
                b -= STEP_MINUS
            else:
                b += STEP_PLUS


            # if b + STEP_PLUS <= TO_STORE:
            #     b += STEP_PLUS
            # else:
            #     b -= STEP_MINUS

        di1[i] = li1
        li1 = []

# print(sorted(di1[1]))
# print(STORES)

# for k, v in di1.items():
#     print(k, v, sorted(v), len(v))
