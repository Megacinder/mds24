from md1_python_basic.assignment1 import (
    n1_the_pencil_shop_wo_input_and_print,
    n2_telegrams,
    n3_the_telephone_number,
    n4_time_for_english,
    n5_interest_on_deposit,
    n6_traffic_lights,
    n7_what_century_is_it,
    n8_aibolits_assistant,
    n9_going_to_the_museum,
    n10_quotas_to_the_olympics,
)
from pytest import fixture


def test_n1_the_pencil_shop_wo_input_and_print():
    for condition, answer in (
        ([19, 100], "5 5"),
        ([20, 100], "5 0"),
        ([20, 10], "0 10"),
        ([3, 10], "3 1"),
    ):
        assert n1_the_pencil_shop_wo_input_and_print(*condition) == answer


def test_n2_telegrams():
    s1 = 'There are exactly'
    s2 = 'characters in'
    for condition, answer in (
        (
            "I beseech you to believe thrown Yalta by Woland's hypnosis Lightning Likhodeyev's identity confirmation",
            "There are exactly 103 characters in: I beseech you to believe thrown Yalta by Woland's hypnosis Lightning Likhodeyev's identity confirmation",
        ),
        ("Mike\\'s", f"{s1} 7 {s2}: Mike\\'s"),
        ("    ", f"{s1} 4 {s2}:     "),
        ("AAASSSDDD", f"{s1} 9 {s2}: AAASSSDDD"),
    ):
        assert n2_telegrams(condition) == answer


def test_n3_the_telephone_number():
    for condition, answer in (
        ("18005550123", "+1(800) 555-01-23"),
        ("00000000000", "+0(000) 000-00-00"),
        ("79266494949", "+7(926) 649-49-49"),
        ("91119911111", "+9(111) 991-11-11"),
    ):
        assert n3_the_telephone_number(condition) == answer


def test_n4_time_for_english():
    for condition, answer in (
        (['2', '30'], "14:30"),
        (['1', '00'], "13:00"),
        (['11', '59'], "23:59"),
    ):
        assert n4_time_for_english(*condition) == answer


def test_n5_interest_on_deposit():
    for condition, answer in (
        ("90000", "300"),
        ("120000", "400"),
        ("9000", "30"),
        ("1", "0"),
    ):
        assert n5_interest_on_deposit(condition) == answer


def test_n6_traffic_lights():
    for condition, answer in (
        ("red", "Please, wait for the permission to cross"),
        ("green", "Crossing is allowed"),
    ):
        assert n6_traffic_lights(condition) == answer


def test_n7_what_century_is_it():
    for condition, answer in (
        ("1", "1"),
        ("2100", "21"),
        ("2000", "20"),
        ("2001", "21"),
    ):
        assert n7_what_century_is_it(condition) == answer


@fixture
def possible_illness():
    return "cough", "temperature"


def test_n8_aibolits_assistant(possible_illness):
    s = "Prescribing you"
    for condition, answer in (
        (['cough', 'wet', possible_illness],         f"{s} Otharkin-1"),
        (['cough', 'dry', possible_illness],         f"{s} Otharkin-2"),
        (['temperature', '38.1', possible_illness],  f"{s} Antifever-1"),
        (['temperature', '37.99', possible_illness], f"{s} Antifever-2"),
        (['temperature', '-999.99', possible_illness], f"{s} Antifever-2"),
        (['huy', 'pizda', possible_illness], "Please see Dr. Aibolit"),
        (['umirau', None, possible_illness], "Please see Dr. Aibolit"),
    ):
        assert n8_aibolits_assistant(*condition) == answer


def test_n9_going_to_the_museum():
    for condition, answer in (
        (['249', None], "No"),
        (['251', 'No'], "No"),
        (['499', 'Yes'], "Yes"),
        (['500', None], "Yes"),
    ):
        assert n9_going_to_the_museum(*condition) == answer


def test_n10_quotas_to_the_olympics():
    for condition, answer in (
        (['15', '9'], "Hurray! The Olympic quotas are won!\n2"),
        (['19', '1'], "Hurray! The Olympic quotas are won!\n3"),
        (['19', '15'], "Hurray! The Olympic quotas are won!\n1"),
        (['1', '1'], "Hurray! The Olympic quotas are won!\n3"),
        (['24', '100'], "Must participate in a qualifying competition"),
        (['100', '20'], "Must participate in a qualifying competition"),
        (['1', '100'], "Must participate in a qualifying competition"),
        (['100', '1'], "Must participate in a qualifying competition"),
        (['100', '100'], "Unfortunately, this time it was not possible to win an Olympics quota"),
        (['-1', '-1'], "Unfortunately, this time it was not possible to win an Olympics quota"),
    ):
        assert n10_quotas_to_the_olympics(*condition) == answer
