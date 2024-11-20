def print_three_outputs_with_colons_and_spaces():
    o1 = input()
    o2 = input()
    o3 = input()
    print(f"{o1} :: {o2} :: {o3}")


def test_just_print_three_int_outputs(monkeypatch, capsys):
    for condition, answer in (
        (['1', '2', '3'], "1 :: 2 :: 3"),
        (['a', 'b', 'c'], "a :: b :: c"),
        (['M', 'D', 'S'], "M :: D :: S"),
        (['Ru', 'le', 'zz'], "Ru :: le :: zz"),
    ):
        inputs = iter(condition)
        monkeypatch.setattr('builtins.input', lambda: next(inputs))
        print_three_outputs_with_colons_and_spaces()
        captured = capsys.readouterr()
        assert captured.out == f'{answer}\n'
