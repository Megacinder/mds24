from md1_python_basic.assignment3 import (
    n1_email,
    n4_expenditures_on_lunches,
    n5_encrypted_message,
    n6_magic_wands,
    n7_club_enrolment,
    n8_cakes_on_instagram,
    n9_kanji,
)
from pytest import fixture


def test_n1_the_pencil_shop_wo_input_and_print():
    for condition, answer in (
        ([
             "arkul@yandex.ru", "barabaw@hse.edu.ru",
             "forgorren_at_sign/?/mail.ru", "online@gmail.com;online234@gmail.com",
         ], 4),
    ):
        assert n1_email(condition) == answer


def test_n4_expenditures_on_lunches():
    for condition, answer in (
        ("299.0, 350.50, 350, 99, 200, 100.99, 80", 99.0),
    ):
        assert n4_expenditures_on_lunches(condition) == answer


def test_n5_encrypted_message():
    for condition, answer in (
        ("ERthisYf SiisTu bEtoole undifficulter", "this is too difficult"),
    ):
        assert n5_encrypted_message(condition) == answer


def test_n6_magic_wands():
    for condition, answer in (
        (
            [
                [
                    "Dolores Umbridge, 1957, birch",
                    "Hermione Granger, 1989, grapevine",
                    "Cedric Diggory, 1985, ash",
                    "Neville Dolgopups, 1989, cherry tree",
                ],
                "1989",
            ],
            (
                "In 1989 the following people bought a wand: "
                + "Hermione Granger (material - grapevine), "
                + "Neville Dolgopups (material - cherry tree)."
            ),
        ),
    ):
        assert n6_magic_wands(*condition) == answer


def test_n7_club_enrolment():
    for condition, answer in (
        (
            [
                "Sergeeva, Larin, Hrabachak, Agapova, Sloyko, Gretzky",
                "Dmitrieva, Agapova, Sergeeva, Terentiev, Grabachak, Sloyko",
                "Gretzky, Terentiev, Dmitrieva, Larin, Grabachak, Sergeeva",
            ],
            1,
        ),
        (["AAA, bbb", "aaa, BBB", "AAA"], 0),
        (["AAA, BBB", "AAA, BBB", "AAA, BBB"], 2),
    ):
        assert n7_club_enrolment(condition) == answer


def test_n8_cakes_on_instagram():
    for condition, answer in (
        (
            [
                [
                    "@nogotochki555",
                    "@mama_leonida",
                    "@papa_leonida",
                    "@ivan_ivanov",
                    "@nogotochki555",
                    "@other_cakes",
                    "@petr_petrov",
                    "@mama_leonida",
                ],
                [
                    "@nogotochki555",
                    "@other_cakes",
                ],
            ],
            4,
        ),

    ):
        assert n8_cakes_on_instagram(*condition) == answer


def test_n9_kanji():
    for condition, answer in (
        (
            [
                [
                    "milk 牛乳",
                    "sun 日",
                    "constitution 憲法",
                    "imitation 模倣",
                ],
                "sun",
            ],
            "日",
        ),
        (
            [
                [
                    "milk 1",
                    "sun 2",
                    "constitution 3",
                    "imitation 4",
                ],
                "sun",
            ],
            "2",
        ),

    ):
        assert n9_kanji(*condition) == answer
