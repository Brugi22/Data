import abc
from typing import Set


class IDetectionAPI(abc.ABC):

    @abc.abstractmethod
    def detect_files(self):
        raise NotImplementedError("method must be implemented")

