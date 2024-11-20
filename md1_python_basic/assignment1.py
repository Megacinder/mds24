N8_POSSIBLE_ILLNESS = ("cough", "temperature")


def n0_just_print_three_outputs():
    o1 = input()
    o2 = input()
    o3 = input()
    print(o1, o2, o3)


def n1_the_pencil_shop():
    n = int(input())
    m = int(input())
    print(f'{m // n} {m % n}')


def n1_the_pencil_shop_wo_input_and_print(n: int, m: int) -> str:
    return f'{m // n} {m % n}'


def n2_telegrams(s: str) -> str:
    return f"There are exactly {len(s)} characters in: {s}"


def n3_the_telephone_number(s: str) -> str:
    return f"+{s[0]}({s[1:4]}) {s[4:7]}-{s[7:9]}-{s[-2:]}"


def n4_time_for_english(hh: str, mi: str) -> str:
    hh = int(hh)
    hh += 12
    return f"{hh}:{mi}"


def n5_interest_on_deposit(dp: str) -> str:
    dp = int(dp)
    pr = 0.04
    return f"{int(dp * pr / 12)}"


def n6_traffic_lights(co: str) -> str:
    return "Crossing is allowed" if co == "green" else "Please, wait for the permission to cross"


def n7_what_century_is_it(yyyy: str) -> str:
    y = int(yyyy)
    a = 0 if y % 100 == 0 else 1
    return f"{y // 100 + a}"


def n8_aibolits_assistant(a: str, b: str, t: tuple) -> str:
    s = "Prescribing you"

    if a not in t:
        s = "Please see Dr. Aibolit"
        return s

    if a == "cough":
        if b == "wet":
            s += " Otharkin-1"
        elif b == "dry":
            s += " Otharkin-2"
    elif a == "temperature":
        if float(b) > 38:
            s += " Antifever-1"
        elif float(b) <= 38:
            s += " Antifever-2"
    else:
        s = "Please see Dr. Aibolit"
    return s


def launch_n8() -> str:
    a = input()
    b = input() if a in N8_POSSIBLE_ILLNESS else ''
    return n8_aibolits_assistant(a, b, N8_POSSIBLE_ILLNESS)


def n9_going_to_the_museum(a: str, b: str = '') -> str:
    a = int(a)
    has_stud_and_money = (250 <= a < 500 and b == 'Yes')

    if a >= 500 or has_stud_and_money:
        return 'Yes'
    else:
        return 'No'


def launch_n9() -> str:
    a = input()

    if int(a) < 250:
        b = 'No'
    elif int(a) >= 500:
        b = 'Yes'
    else:
        b = input()

    return n9_going_to_the_museum(a, b)


def n10_quotas_to_the_olympics(wc: str, ec: str) -> str:
    wc = int(wc)
    ec = int(ec)

    if (1 <= wc <= 20) and (1 <= ec <= 16):
        s = "Hurray! The Olympic quotas are won!"
        if 1 <= wc <= 2 or 1 <= ec <= 2:
            s += '\n3'
            return s
        elif (1 <= wc <= 10) or (1 <= ec <= 10):
            s += '\n2'
            return s
        else:
            s += '\n1'
            return s
    elif (1 <= wc <= 24) or (1 <= ec <= 20):
        s = 'Must participate in a qualifying competition'
        return s

    s = 'Unfortunately, this time it was not possible to win an Olympics quota'
    return s


if __name__ == "__main__":
    wc = input()
    ec = input()
    print(n10_quotas_to_the_olympics(wc, ec))
