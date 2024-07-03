from typing import List, Tuple, Iterable

from asammdf import MDF

from prefect_implementation.quality_check.IQualityCheck import IQualityCheck

class QualityCheck(IQualityCheck):
    def check_quality(self, paths: Iterable[str]) -> Tuple[List[str], List[str]]:
        valid_files = []
        invalid_files = []
        for path in paths:
            try:
                mdf_obj = MDF(path)
                valid_files.append(path)
            except Exception as e:
                invalid_files.append(path)
                print(f"Invalid MDF file: {path}. Reason: {e}")
        return valid_files, invalid_files
            