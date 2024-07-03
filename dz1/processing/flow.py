import os
import pickle
from pprint import pprint
from typing import Iterable

from pandas import DataFrame
from asammdf import MDF

from processing.calculators import YourCalculator
from processing.models import Event, Results


def get_events(df: DataFrame, file_path: str) -> Iterable[Event]:
    events = []
    start_time = None

    df = df.dropna(subset=['SPEED'])
    for index, row in df.iterrows():
        if row['SPEED'] >= 40 and row['SPEED'] <= 60:
            if start_time is None:
                start_time = index
        else:
            if start_time is not None:
                events.append(Event(start_time, index, file_path))
                start_time = None

    if start_time is not None:
        events.append(Event(start_time, df.iloc[-1].index, file_path))
    return events

def get_dataframe_from_mf4(file_path: str) -> DataFrame:
    mdf_obj = MDF(file_path)
    return mdf_obj.to_dataframe()

def processing_flow_logic(file_path: str, result_output_path: str):
    dataframe = get_dataframe_from_mf4(file_path)
    events = get_events(dataframe, file_path)
    calculator = YourCalculator()

    calculated = {}
    for event in events:
        try:
            calculated[hash(event)] = calculator.calculate(dataframe, event, ['min', 'max', 'mean', 'std'])
        except Exception as e:
            print(f"Could not calculate for event on file {event.file}. Reason: {e}!")
            pass

    result = Results(events, calculated)

    with open(f'{result_output_path}\\{file_path.split("\\")[-1]}.pickle', 'wb') as handle:
        pickle.dump(result, handle, protocol=pickle.HIGHEST_PROTOCOL)
