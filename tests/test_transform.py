# content of test_sample.py
def inc(x):
    return x + 1

# Supposed to fail
def test_answer():
    assert inc(3) == 4