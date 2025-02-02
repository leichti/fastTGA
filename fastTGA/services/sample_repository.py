import os
from typing import List, Dict, Any

import polars as pl


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

    def select_single(self, sample_id: str) -> pl.DataFrame:
        """
        Returns a single sample DataFrame for the given sample_id if it exists in the (filtered) metadata.
        Returns None if the given sample_id is not found.

        Args:
            sample_id (str): The sample id to select.

        Returns:
            pl.DataFrame: Sample data or None if not found.
        """
        filtered = self._get_filtered_metadata().filter(pl.col("id") == sample_id)
        if filtered.height == 0:
            return None
        return self._get_sample_df(sample_id)

    def select_multiple(self, key: str, values: List[str]) -> List[Dict[str, Any]]:
        """
        For each value in `values`, if a matching row is found in the (filtered) metadata (using the provided key),
        then the corresponding sample file is loaded.

        Args:
            key (str): The column name in the metadata to search (typically "id").
            values (List[str]): List of values to look up.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries with keys "id" and "df".
        """
        results = []
        filtered = self._get_filtered_metadata()
        for val in values:
            if filtered.filter(pl.col(key) == val).height > 0:
                sample_df = self._get_sample_df(val)
                results.append({"id": val, "df": sample_df})
        return results

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


def select(self) -> List[Dict[str, Any]]:
    """
    Iterates over the filtered metadata rows, loads each corresponding sample DataFrame,
    and returns a list of dictionaries with keys "id" and "data" where:
      - "id" is the sample identifier.
      - "data" is the sample DataFrame loaded from the corresponding file.

    Returns:
        List[Dict[str, Any]]: List of dictionaries containing sample ids and their data.

    Raises:
        KeyError: If the 'id' column is missing in the metadata.
    """
    filtered = self._get_filtered_metadata()
    results = []
    id_col = "id"

    if id_col not in filtered.columns:
        raise KeyError(f"Metadata does not contain expected '{id_col}' column.")

    # Iterate over the sample ids in the filtered metadata
    for sample_id in filtered[id_col]:
        try:
            # Ensure we convert the sample_id to a string if needed
            sample_df = self._get_sample_df(str(sample_id))
            results.append({"id": sample_id, "data": sample_df})
        except FileNotFoundError:
            print(f"Warning: Sample file for id = {sample_id} not found. Skipping.")
    return results

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