import os
from typing import Iterable

from prefect import flow
from prefect.deployments import Deployment

from prefect_implementation.detection.impl.FileDetector import FileDetector
from processing.flow import processing_flow_logic
from prefect_implementation.quality_check.impl.QualityCheck import QualityCheck

file_source = "data"
detection_api = FileDetector(files_sources=[file_source])
quality_checker = QualityCheck()


@flow(log_prints=True)
def detect_files() -> Iterable[str]:
    detected = detection_api.detect_files()

    return detected


@flow(name="deploy processing", log_prints=True)
def processing_deployment(paths: Iterable[str], result_output_path: str):
    for path in paths:
        processing_flow_logic(path, result_output_path)


@flow(log_prints=True)
def main_flow(result_output_path: str):
    detected_files = detect_files()
    print(f"detected: {len(detected_files)}")

    valid_files, invalid_files = quality_checker.check_quality(detected_files)
    print(f"Valid files: {len(valid_files)}, Invalid files: {len(invalid_files)}")

    processing_deployment(valid_files, result_output_path)


if __name__ == '__main__':
    main_flow("data\\output")

    main_flow_instance = Deployment.build_from_flow(name="Dz1_flow",
                                                    flow=main_flow,
                                                    path=os.path.abspath(os.path.curdir),
                                                    work_queue_name="demo",
                                                    work_pool_name="Agent")
    main_flow_instance.apply()