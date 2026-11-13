import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from common import oee


def test_perfect_run():
    assert oee(480, 480, 960, 0, 30) == 1.0


def test_known_value():
    # A = 400/500, P = 30*600/(400*60), Q = 570/600
    expected = (400 / 500) * (30 * 600 / 24000) * (570 / 600)
    assert abs(oee(500, 400, 600, 30, 30) - expected) < 1e-9


def test_zero_guard():
    assert oee(480, 0, 0, 0, 30) == 0.0
