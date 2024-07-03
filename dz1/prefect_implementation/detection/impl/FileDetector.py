import os
from typing import Set, Iterable

from prefect_implementation.detection.IDetectionAPI import IDetectionAPI


class FileDetector(IDetectionAPI):

    def __init__(self, files_sources: Iterable[str]):
        self.files_sources = files_sources

    def detect_files(self) -> Iterable[str]:
        detected_files = set() 
        for source in self.files_sources:
            if os.path.isdir(source):
                for root, dirs, files in os.walk(source):
                    for file in files:
                        detected_files.add(os.path.join(root, file))
            else:
                print(f"Warning: {source} is not a valid directory.")
        return detected_files
