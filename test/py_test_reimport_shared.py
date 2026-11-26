"""Listed in a reimport template's `imports`: shared by every run."""

from dataclasses import dataclass

COUNT = {'n': 0}


@dataclass
class Pair:
    a: int
    b: int


def bump():
    COUNT['n'] += 1
    return COUNT['n']
