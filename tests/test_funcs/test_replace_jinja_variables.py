from sesam import SesamCmdClient
from tests.args import Args


def test_replace_jinja_variables():
    test_data = "This is a {{@ test1 @}} {{@ test2 @}} replacement test"
    expected_data = b"This is a jinja variable replacement test"

    args = Args()

    # Dict of vars to replace in the passed text
    args.jinja_vars = {"test1": "jinja", "test2": "variable"}
    cmdClient = SesamCmdClient(args, args.logger)

    processed = cmdClient.replace_jinja_variables(test_data)

    assert processed == expected_data
