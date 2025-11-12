from filesage.cli import greet

def test_greet_default():
    assert greet() == "Hello Filesage"

def test_greet_custom():
    assert greet("Brett") == "Hello Brett"
