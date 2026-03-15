from enum import Enum, auto


class CollectStatus(Enum):
    SUCCESS = auto()
    UNAVAILABLE = auto()
    FAILED = auto()


__all__ = [
    "CollectStatus",
]
