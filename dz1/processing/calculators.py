from abc import abstractmethod
from typing import List, Dict

from pandas import DataFrame

from processing.models import Event


class ICalculator:
    def calculate(self, dataframe: DataFrame, event: Event, wanted_channels: List[str]) -> Dict[str, float]:
        dataframe_event_chunk = dataframe.query(f"{event.start}<index<{event.end}")
        result = self.calculate_on_dataframe_chunk(dataframe_event_chunk)
        return result.get(wanted_channels).to_dict()

    @abstractmethod
    def calculate_on_dataframe_chunk(self, dataframe_chunk: DataFrame) -> DataFrame:
        raise NotImplementedError("Implement this abstract method in child classes!")


class YourCalculator(ICalculator):
    def calculate_on_dataframe_chunk(self, dataframe_chunk: DataFrame) -> DataFrame:
        min_values = dataframe_chunk.min()
        max_values = dataframe_chunk.max()
        mean_values = dataframe_chunk.mean()
        std_values = dataframe_chunk.std()

        summary_df = DataFrame({
            'min': min_values,
            'max': max_values,
            'mean': mean_values,
            'std': std_values
        })

        return summary_df



