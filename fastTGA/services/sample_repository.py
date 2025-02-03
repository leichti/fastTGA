import os
from typing import List, Dict, Any

import polars as pl

from fastTGA.services.data_filters import DataFilter


class SampleRepository:
    def __init__(self, folder_path: str):
        """
        Initializes the repository with the path to the folder.
        The folder must contain:
          • metadata.parquet – A metadata file with an 'id' column and other sample info.
          • sample_{id}.parquet – Files containing sample data, where {id} corresponds to the metadata 'id' value.

        Args:
            folder_path (str): Path to the folder.
        """
        self.folder_path = folder_path
        self.metadata_file = os.path.join(folder_path, "metadata.parquet")
        self.metadata = pl.read_parquet(self.metadata_file)
        self._filters: List[pl.Expr] = []
        self.data_filters: List[DataFilter] = []

    def filter(self, column: str, value, operator: str = None) -> "SampleRepository":
        """
        Adds a filter condition based on a column, value, and an optional operator.
        Filters can be chained.

        If operator is provided (e.g., ">", "<", ">=", "<=", "==", "!="), the condition is applied.
        If no operator is provided:
          • When the value is a list then an "in" check is used.
          • Otherwise, an equality check is performed.

        Args:
            column (str): Metadata column name to filter on.
            value: Value or list of values to compare.
            operator (str, optional): Comparison operator.

        Returns:
            SampleRepository: Self instance to allow method chaining.
        """
        col_expr = pl.col(column)
        expr = None

        if operator is not None:
            if operator == ">":
                expr = col_expr > value
            elif operator == "<":
                expr = col_expr < value
            elif operator == ">=":
                expr = col_expr >= value
            elif operator == "<=":
                expr = col_expr <= value
            elif operator in {"==", "="}:
                expr = col_expr == value
            elif operator == "!=":
                expr = col_expr != value
            else:
                raise ValueError(f"Unsupported operator: {operator}")
        else:
            if isinstance(value, list):
                expr = col_expr.is_in(value)
            else:
                expr = col_expr == value

        self._filters.append(expr)
        return self

    def hide_multiple(self, values: List[str]) -> "SampleRepository":
        """
        Hides samples whose 'id' is in the provided list.
        This method is a specific version of a filter, adding a condition that excludes rows
        where 'id' is one of the specified values.

        Args:
            values (List[str]): List of sample ids to hide.

        Returns:
            SampleRepository: The repository instance for method chaining.
        """
        self._filters.append(~pl.col("id").is_in(values))
        return self

    def reset_filters(self) -> "SampleRepository":
        """
        Resets all applied filters.

        Returns:
            SampleRepository: Self instance.
        """
        self._filters = []
        return self

    def _get_filtered_metadata(self) -> pl.DataFrame:
        """
        Applies all stored filters on the metadata DataFrame and returns the filtered DataFrame.

        Returns:
            pl.DataFrame: The metadata filtered according to the applied conditions.
        """
        if self._filters:
            combined_expr = self._filters[0]
            for expr in self._filters[1:]:
                combined_expr = combined_expr & expr
            return self.metadata.filter(combined_expr)
        return self.metadata

    def _get_sample_df(self, sample_id: str) -> pl.DataFrame:
        """
        Loads and returns a sample DataFrame corresponding to the given sample_id.

        Args:
            sample_id (str): The value from the metadata 'id' column used to load the corresponding sample file.

        Returns:
            pl.DataFrame: The sample data loaded from its parquet file.

        Raises:
            FileNotFoundError: If the sample file does not exist.
        """
        sample_path = os.path.join(self.folder_path, f"sample_{sample_id}.parquet")
        if not os.path.exists(sample_path):
            raise FileNotFoundError(f"Sample file not found: {sample_path}")
        return pl.read_parquet(sample_path)

    def head(self, n: int = 5) -> pl.DataFrame:
        """
        Returns the first n rows of the (filtered) metadata DataFrame.

        Args:
            n (int, optional): Number of rows to return. Defaults to 5.

        Returns:
            pl.DataFrame: The first n rows.
        """
        pl.Config.set_tbl_cols(0)
        return self._get_filtered_metadata().head(n)

    def columns_info(self) -> Dict[str, Any]:
        """
        Returns a dictionary with column names as keys and an example value (from the first row)
        to help inspect which columns are available.

        Returns:
            Dict[str, Any]: Mapping of each column to an example value.
        """
        filtered = self._get_filtered_metadata()
        info = {}
        if filtered.height == 0:
            return info

        first_row = filtered.row(0)
        for col, val in zip(filtered.columns, first_row):
            info[col] = val
        return info

    def unique_columns(self) -> Dict[str, List[Any]]:
        """
        Returns a dictionary where each key is a column name and the corresponding value
        is a list of the unique values found in that column in the filtered metadata.

        Returns:
            Dict[str, List[Any]]: Mapping of each column to its unique values.
        """
        filtered = self._get_filtered_metadata()
        unique_info = {}
        for col in filtered.columns:
            # Convert the unique series to list for readability
            unique_info[col] = filtered.select(pl.col(col)).unique().to_series().to_list()
        return unique_info

    def print_uniques(self):
        print("\nUnique Column Values:")
        unique_cols = self.unique_columns()
        for col, uniques in unique_cols.items():
            print(f"{col}: {uniques}")

    def match_time_column_for_temperature(
            self,
            target_temperature: float,
            time_column: str = "t_s",
            temperature_column: str = "T_C"
    ) -> "SampleRepository":
        """
        Sets the time matching settings for the repository so that subsequent sample loading
        (via select_single, select_multiple, or select) will adjust the sample DataFrames such that
        the new time column (time_column) is shifted so that t_s = 0 is at the moment when the temperature
        (temperature_column) is closest to target_temperature.

        Args:
            target_temperature (float): The target temperature used to align the time columns.
            time_column (str, optional): The name of the new time column to store the shifted time. Default is "t_s".
            temperature_column (str, optional): The name of the temperature column in the sample data. Default is "T_C".

        Returns:
            SampleRepository: The repository instance (for potential method chaining).
        """
        self._time_matching_settings = {
            "target_temperature": target_temperature,
            "time_column": time_column,
            "temperature_column": temperature_column,
        }
        return self

    def _adjust_time_column(
            self,
            sample_df: pl.DataFrame,
            target_temperature: float,
            time_column: str = "t_s",
            temperature_column: str = "T_C"
    ) -> pl.DataFrame:
        """
        Adjusts the time column in a given sample DataFrame based on a target temperature.

        It finds the row where the temperature (from temperature_column) is closest to the target_temperature,
        then computes a new time shift such that t_s=0 at that row. A new column 't_s' is appended to the DataFrame,
        which equals (original time - time at closest temperature).

        Args:
            sample_df (pl.DataFrame): The sample DataFrame.
            target_temperature (float): The target temperature value.
            time_column (str, optional): Name of the time column. Defaults to "time".
            temperature_column (str, optional): Name of the temperature column. Defaults to "temperature".

        Returns:
            pl.DataFrame: A new DataFrame with an added 't_s' column.
        """
        # Compute the absolute difference between the temperature values and the target temperature.
        diff_series = (sample_df[temperature_column] - target_temperature).abs()

        # Find the index (row) at which this difference is minimal.
        idx = diff_series.arg_min()

        # Get the time value at this index in order to use it as a shift anchor.
        time_at_target = sample_df[time_column][idx]

        # Create a new DataFrame with an additional column "t_s" for the shifted time.
        adjusted_df = sample_df.with_columns((pl.col(time_column) - time_at_target).alias("t_s"))
        return adjusted_df

    def _load_sample_df(self, sample_id: str) -> pl.DataFrame:
        """
        Loads a sample DataFrame based on its id and applies the time column adjustment
        if time matching settings have been configured (via match_time_column_for_temperature).

        Args:
            sample_id (str): The sample identifier.

        Returns:
            pl.DataFrame: The (possibly) adjusted sample DataFrame.
        """
        sample_df = self._get_sample_df(sample_id)
        # If time matching settings have been applied, adjust the time column:
        if hasattr(self, "_time_matching_settings") and self._time_matching_settings:
            s = self._time_matching_settings
            sample_df = self._adjust_time_column(
                sample_df,
                s["target_temperature"],
                s["time_column"],
                s["temperature_column"]
            )
        sample_df = self._apply_data_filters(sample_df)
        return sample_df

    # Updated select_single to use _load_sample_df instead of _get_sample_df:
    def select_single(self, sample_id: str) -> pl.DataFrame:
        """
        Returns a single sample DataFrame for the given sample_id if it exists in the (filtered) metadata.
        If the repository was configured to adjust the time column (via match_time_column_for_temperature),
        the returned DataFrame will have the adjusted time column.

        Args:
            sample_id (str): The sample id to select.

        Returns:
            pl.DataFrame: Sample data or None if not found.
        """
        filtered = self._get_filtered_metadata().filter(pl.col("id") == sample_id)
        if filtered.height == 0:
            return None
        return self._load_sample_df(str(sample_id))

    def get_metadata_for_id(self, sample_id: str) -> dict:
        """
        Returns the metadata row for a given sample id.

        Args:
            sample_id (str): The sample id to look up.

        Returns:
            pl.DataFrame: The metadata row for the given sample id.
        """
        return self.metadata.filter(pl.col("id") == sample_id).to_dict()

    # Updated select_multiple to use _load_sample_df:
    def select_multiple(self, key: str, values: List[str]) -> List[Dict[str, Any]]:
        """
        For each value in `values`, if a matching row is found in the (filtered) metadata (using the provided key),
        then the corresponding sample file is loaded and adjusted (if matching settings exist).

        Args:
            key (str): The column name in the metadata to search (typically "id").
            values (List[str]): List of values to look up.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries with keys "id" and "data".
        """
        results = []
        filtered = self._get_filtered_metadata()
        for val in values:
            if filtered.filter(pl.col(key) == val).height > 0:
                sample_df = self._load_sample_df(str(val))
                results.append({"id": val, "data": sample_df})
        return results


# Updated select to use _load_sample_df:
    def select(self) -> List[Dict[str, Any]]:
        """
        Iterates over the filtered metadata rows, loads each corresponding sample DataFrame (applying
        time column adjustments if match_time_column_for_temperature was previously called), and returns
        a list of dictionaries with keys "id" and "data".

        Returns:
            List[Dict[str, Any]]: List of dictionaries containing sample ids and their (adjusted) data.
        """
        results = []
        filtered = self._get_filtered_metadata()
        id_col = "id"

        if id_col not in filtered.columns:
            raise KeyError(f"Metadata does not contain expected '{id_col}' column.")

        # Iterate over the sample ids in the filtered metadata
        for sample_id in filtered[id_col]:
            try:
                adjusted_df = self._load_sample_df(str(sample_id))
                results.append({"id": sample_id, "data": adjusted_df})
            except FileNotFoundError:
                print(f"Warning: Sample file for id '{sample_id}' not found. Skipping.")
        return results

    def data_filter(self, data_filter: DataFilter) -> "SampleRepository":
        """
        Registers a DataFilter to be applied on sample data.
        Multiple filters can be chained.

        Args:
            data_filter (DataFilter): An instance of a DataFilter.

        Returns:
            SampleRepository: self, for method chaining.
        """
        self.data_filters.append(data_filter)
        return self

    def _apply_data_filters(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Passes the sample DataFrame sequentially through all registered data filters.
        """
        for filt in self.data_filters:
            df = filt.apply(df)
        return df

# === Example usage ===
if __name__ == "__main__":
    # Adjust the path to your folder containing metadata.parquet and sample_*.parquet files.
    repo = SampleRepository('/Users/manuelleuchtenmuller/Library/CloudStorage/OneDrive-HydrogenReductionLab/H2Lab Projects/H2Lab_D2V_24_9 Melting Behaviour/TGA/export')

    # Example: chaining filters as needed.
    repo.filter("Sample", "EAFD9").filter("Sample Condition", "Washed")
    repo.print_uniques()

    # Inspect the filtered metadata: first few rows.
    #print("Metadata Preview:")
    #print(repo.head(5))

    # Columns available and an example value from each.
    #print("\nColumns Information:")
    #for col, exemplar in repo.columns_info().items():
    #    print(f"{col}: {exemplar}")

    # Unique values per column.

    # Get a single sample by id.
    sample_df = repo.select_single("RT13")
    if sample_df is not None:
        print("\nSingle Sample 'RT13' Preview:")
        print(sample_df.head())
    else:
        print("\nSample 'RT13' not found!")

    # Get multiple samples, e.g., by id.
    samples = repo.select_multiple("id", ["RT13", "RT14"])
    print("\nMultiple Samples:")
    for item in samples:
        print(f"ID: {item['id']} Sample Preview:")
        print(item["df"].head())