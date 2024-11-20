from md1_python_basic.assignment4 import (
    n2_number_of_visitors_to_the_cafe,
    n3_music_library,
    n4_grammys_award,
    n7_a_telegram_from_matroskin_2,
)
from pytest import fixture


def test_n2_number_of_visitors_to_the_cafe():
    for condition, answer in (
        (
            [
                [
                    "0, 13, 4, 1, 1, 15, 4, 13, 8, 5, 7, 0, 7, 10, 12, 4, 0, 4, 4, 9, 14, 6, 7, 10",
                    "13, 10, 15, 1, 15, 15, 1, 0, 1, 7, 1, 2, 8, 5, 5, 5, 15, 14, 2, 11, 11, 7, 3, 13",
                    "13, 1, 2, 4, 2, 3, 10, 13, 15, 5, 6, 4, 15, 12, 4, 13, 4, 12, 7, 5, 13, 7, 14, 4",
                ],
                2,
            ],
            ["4", "15", "2"]
        ),
    ):
        assert n2_number_of_visitors_to_the_cafe(*condition) == answer


def test_n3_music_library():
    for condition, answer in (
        (
            [
                [
                    "Queen: Innuendo",
                    "Queen: The Miracle",
                    "Dawid Bowie: Space Oddity",
                    "Queen: A Night at the Opera",
                    "Simon & Garfunkel: The Sound of Silence",
                ],
                "Queen",
            ],
            "Queen (3): A Night at the Opera, Innuendo, The Miracle."
        ),
    ):
        assert n3_music_library(*condition) == answer


def test_n4_grammys_award():
    for condition, answer in (
        (
            [
                "Simon & Garfunkel",
                "Queen",
                "David Bowie",
                "Simon & Garfunkel",
                "David Bowie",
                "Simon & Garfunkel",
            ],
            "Simon & Garfunkel: 3\nQueen: 1",
        ),
        # (
        #     ["AAA"],
        #     "AAA: 1\nAAA: 1",
        # ),
        (
            ["AAA", "AAA", "AAA", "AAA", "BBB", "BBB", "BBB", "BBB"],
            "BBB: 4\nAAA: 4",
        ),
        (
            ["AA", "AA", "BB", "BB", "CC", "CC", "DD", "DD"],
            "DD: 2\nAA: 2",
        ),

    ):
        assert n4_grammys_award(condition) == answer


def test_n7_a_telegram_from_matroskin_2():
    for condition, answer in (
        (
            '''"WHAT'S A CREW FOR IN A TOWN THAT DOESN'T EVEN HAVE FOUR THOUSAND INHABITANTS? DOWN WITH THE POPE! (THINGS WITH ROME WERE GETTING TANGLED.) I AM FOR CAESAR, AND ONLY FOR CAESAR. ET CETERA, ET CETERA."''',
            "WHATS A CREW FOR IN A TOWN THAT DOESNT EVEN HAVE FOUR THOUSAND INHABITANTS DOWN WITH THE POPE THINGS WITH ROME WERE GETTING TANGLED I AM FOR CAESAR AND ONLY FOR CAESAR ET CETERA ET CETERA",
        ),
    ):
        assert n7_a_telegram_from_matroskin_2(condition) == answer
