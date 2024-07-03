import abc
from typing import Set


class IQualityCheck(abc.ABC):

    @abc.abstractmethod
    def check_quality(self, path: str):
        raise NotImplementedError("method must be implemented")
