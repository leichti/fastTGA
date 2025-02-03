# Define a DataFilter interface that all data filters should implement.
from abc import abstractmethod, ABC
import polars as pl

class DataFilter(ABC):
    @abstractmethod
    def apply(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Apply this filter on the given DataFrame.

        Args:
            df (pl.DataFrame): The DataFrame to filter.

        Returns:
            pl.DataFrame: The filtered DataFrame.
        """
        pass

# A concrete implementation for filtering samples based on a threshold on a given column.
class CutAwayPurgeFreezingFilter(DataFilter):
    def __init__(self, col: str, threshold: float):
        self.col = col
        self.threshold = threshold

    def apply(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Only keep rows for which the value in self.col is less than self.threshold.
        """
        return df.filter(pl.col(self.col) < self.threshold)