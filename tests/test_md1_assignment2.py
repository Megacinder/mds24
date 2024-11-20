from md1_python_basic.assignment2 import (
    n1_deadline,
    n5_expenses_on_snacks,
    n6_word_search,
    n7_password,
    n8_game_cities_of_the_world,
    n9_house_chores_2,
    n10_athletes,
)
from pytest import fixture


def test_n1_the_pencil_shop_wo_input_and_print():
    for condition, answer in (
        (5, [
            'Hours to deadline: 5', 'Hours to deadline: 4', 'Hours to deadline: 3',
            'Hours to deadline: 2', 'Hours to deadline: 1', 'Time to hand in your work!'
        ]),
        (0, ['Time to hand in your work!']),
        (1, ['Hours to deadline: 1', 'Time to hand in your work!']),
        (-1, ['Time to hand in your work!']),
    ):
        assert n1_deadline(condition) == answer


def test_n5_expenses_on_snacks():
    for condition, answer in (
        ([1, 2, 3, 4], 1),
        ([], 0),
        ([800, 900, 850, 900, 800, 850], 2),
    ):
        assert n5_expenses_on_snacks(condition) == answer


def test_n6_word_search():
    for condition, answer in (
        (["To be or not to be", "That is the question", 7], "That"),
        (["To be or not to be", "That is the question", -1], "There is no such word"),
        (["To", "", 1], "To"),
        (["To", "", 100], "There is no such word"),
        (["AA_SS_DD_FF", "", 0], "There is no such word"),
    ):
        assert n6_word_search(*condition) == answer


def test_n7_password():
    for condition, answer in (
        (["1990", "kotik1990seriy kotikseriy k1993seriy"], ["True", "False", "False"]),
        (["900", "990 99000 aaasss"], ["False", "True", "False"]),
        (["1980", "19gfd80 ffff1980"], ["False", "True"]),
    ):
        assert n7_password(*condition) == answer


def test_n8_game_cities_of_the_world():
    for condition, answer in (
        (
            ["Moscow, Kiev, Tokyo, Seoul, Kazan, London, Paris, Zurich, Milan, Malaga, Lisbon, Oslo, Liverpool", "L"],
            ["London", "Lisbon", "Liverpool"]
        ),
        (["A, B, C, D, E", "A"], ["A"]),
        (["A, B, C, D, E", "Z"], []),
    ):
        assert n8_game_cities_of_the_world(*condition) == answer


def test_n9_house_chores_2():
    for condition, answer in (
        (
            ["defrost the fridge, return books to the library, boil sausages, mop the floor, walk the dog, buy tickets"],
            [
                "return books to the library, mop the floor, buy tickets",
                "defrost the fridge, boil sausages, walk the dog",
            ]
        ),
        (["A, B, C, D, E"], ["B, D", "A, C, E"]),
        (["A, B, C, D, E, F"], ["B, D, F", "A, C, E"]),
    ):
        assert n9_house_chores_2(*condition) == answer


def test_n10_athletes():
    for condition, answer in (
        (["Chen Hanyu Zhou Uno Jin Kolyada", "2 6 10"], ["2 place: Hanyu", "6 place: Kolyada", "10 place: No athlete"]),
        (["A B C D E", "1 2 3"], ["1 place: A", "2 place: B", "3 place: C"]),
        (["A B C D E F", "11 22 33"], ["11 place: No athlete", "22 place: No athlete", "33 place: No athlete"]),
    ):
        assert n10_athletes(*condition) == answer
