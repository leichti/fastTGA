import os
import polars as pl
from PyQt6.QtCore import QObject, pyqtSignal

from models.google_spreadsheet_model import GoogleSpreadsheetModel
from models.tga_file import TGAFile
from models.txt_directory_model import TXTDirectoryModel


class TableModel(QObject):
    """
    Simplified TableModel that:
      1. Reads metadata from 'metadata.parquet' at construction if available.
      2. Writes metadata automatically whenever entries are added (configurable via save=True).
      3. Stores TGA data in 'sample_<id>.parquet' for each sample.
    """

    error_occurred = pyqtSignal(str)

    def __init__(self, path_to_directory=None):
        """
        :param path_to_directory: directory path where metadata and TGA .parquet files will be stored.
        """
        super().__init__()
        self.path_to_directory = path_to_directory
        if not os.path.exists(self.path_to_directory):
            os.makedirs(self.path_to_directory, exist_ok=True)

        self.metadata_file = os.path.join(self.path_to_directory, "metadata.parquet")

        # Attempt to load existing metadata
        if os.path.exists(self.metadata_file):
            self.metadata_table = pl.read_parquet(self.metadata_file)
        else:
            # Start with an empty DataFrame
            self.metadata_table = pl.DataFrame()

    def save_metadata(self):
        """
        Write the current in-memory metadata table to disk.
        """
        if not self.metadata_table.is_empty():
            self.metadata_table.write_parquet(self.metadata_file)
            print(f"Metadata saved to {self.metadata_file}")

    def prepare_entry_data(self, tga_file_dict, gspread_model: GoogleSpreadsheetModel):
        """
        1. Create a TGAFile instance from the path in tga_file_dict.
        2. Look up the metadata from gspread_model using either 'id' or 'name'.
        3. Return (tga_file, metadata) or signal an error if not found.
        """
        file_id = tga_file_dict["id"]
        file_path = tga_file_dict["path"]

        tga_file = TGAFile(file_path)  # Loads Polars DataFrame in tga_file.data

        # Attempt to look up metadata by ID first
        metadata = gspread_model.get_metadata(file_id)
        if not metadata:
            # fallback: possibly use 'name' field in tga_file.metadata
            name_in_file = tga_file.metadata.get("name", "")
            metadata = gspread_model.get_metadata(name_in_file)

        if not metadata:
            self.error_occurred.emit("Metadata not found for file: " + file_path)
            return None, None

        return tga_file, metadata

    def add_entry(self, tga_file: TGAFile, gspread_model: GoogleSpreadsheetModel, save: bool = True):
        """
        1. Merge metadata from gspread_model.table_df (Polars) and tga_file.metadata (dict).
        2. Write TGA data to 'sample_<id>.parquet'.
        3. Update metadata_table with the merged metadata (one row for this sample_id).
        4. If save=True, save the metadata to disk immediately. Otherwise, wait until user calls save_metadata().
        """
        sample_id = tga_file.id
        tga_df = tga_file.data

        # Filter any row from gspread_model.table_df by sample_id
        gspread_dict = gspread_model.get_metadata(sample_id)

        # Merge with the TGA-file-specific metadata
        combined_metadata = {**gspread_dict, **tga_file.metadata}
        combined_metadata["id"] = sample_id  # ensure 'id' field is set

        # Convert to single-row Polars DataFrame
        new_row = pl.DataFrame([combined_metadata])

        # Write TGA data to disk (one file per sample)
        sample_parquet = os.path.join(self.path_to_directory, f"sample_{sample_id}.parquet")
        if os.path.exists(sample_parquet):
            os.remove(sample_parquet)
        tga_df.write_parquet(sample_parquet, compression="gzip")

        # Remove any old row for this sample from metadata_table
        if not self.metadata_table.is_empty():
            mask = self.metadata_table["id"] == sample_id
            if mask.any():
                self.metadata_table = self.metadata_table.filter(~mask)

        # Append the new row to the in-memory table
        self.metadata_table = pl.concat([self.metadata_table, new_row], rechunk=True)

        # Save metadata to disk now if requested
        if save:
            self.save_metadata()
        print(f"Added/updated entry: {sample_id}")

    def read_entry(self, sample_id: str) -> pl.DataFrame | None:
        """
        Load the TGA data for the given sample.
        """
        sample_parquet = os.path.join(self.path_to_directory, f"sample_{sample_id}.parquet")
        if os.path.exists(sample_parquet):
            return pl.read_parquet(sample_parquet)
        else:
            self.error_occurred.emit(f"Sample {sample_id} not found")
            return None

    def find_metadata(self, sample_id: str) -> pl.DataFrame:
        """
        Return the metadata row for the given ID as a Polars DataFrame, or empty if none found.
        """
        if self.metadata_table.is_empty():
            return pl.DataFrame()

        mask = self.metadata_table["id"] == sample_id
        return self.metadata_table.filter(mask)

    def create(self, gspread_model: GoogleSpreadsheetModel, txtmodel: TXTDirectoryModel):
        """
        Iterate over each TGA file from txtmodel, prepare entry data, and add them.
        By default, each add_entry call triggers a save (can be changed by passing save=False).
        """
        for file_info in txtmodel.txt_files:
            tga_file, metadata = self.prepare_entry_data(file_info, gspread_model)
            if tga_file is not None and metadata is not None:
                self.add_entry(tga_file, gspread_model, save=True)

    # ─────────────────────────────────────────────────────────────────────────────
    # Single-condition find (with optional comparator, default "==")
    # ─────────────────────────────────────────────────────────────────────────────
    def find(self, column_name: str, value, operator: str = "==") -> list[dict]:
        """
        Filter the metadata table by a single condition: <column_name> <operator> value.
        Supported operators: ==, !=, >, <, >=, <=.

        Returns a list of dicts, each containing:
          {
            "metadata": <Python dict of that row>,
            "data": <Polars DataFrame of TGA data or None>
          }
        """
        allowed_ops = {
            "==": lambda c, v: c == v,
            "!=": lambda c, v: c != v,
            ">": lambda c, v: c > v,
            "<": lambda c, v: c < v,
            ">=": lambda c, v: c >= v,
            "<=": lambda c, v: c <= v,
        }
        if operator not in allowed_ops:
            self.error_occurred.emit(f"Operator '{operator}' not supported. Using '=='.")
            operator = "=="

        if column_name not in self.metadata_table.columns:
            self.error_occurred.emit(f"Column '{column_name}' not found in metadata.")
            return []

        expr = allowed_ops[operator](pl.col(column_name), value)
        filtered = self.metadata_table.filter(expr)

        results = []
        for row_dict in filtered.to_dicts():
            sample_id = row_dict.get("id")
            if sample_id is None:
                continue
            tga_data = self.read_entry(sample_id)
            results.append({
                "metadata": row_dict,
                "data": tga_data
            })
        return results

    # ─────────────────────────────────────────────────────────────────────────────
    # Multi-condition find_all (AND logic)
    # ─────────────────────────────────────────────────────────────────────────────
    def find_all(self, conditions: list[tuple[str, str, object]]) -> list[dict]:
        """
        Accepts a list of conditions, each a tuple (column_name, operator, value).
        Combines them with logical AND. E.g.:
            [
                ("Sample Condition", "==", "Washed"),
                ("Temperature", ">", 300),
            ]
        Returns a list of dicts: { "metadata": ..., "data": ... } for each match.
        """
        allowed_ops = {
            "==": lambda c, v: c == v,
            "!=": lambda c, v: c != v,
            ">": lambda c, v: c > v,
            "<": lambda c, v: c < v,
            ">=": lambda c, v: c >= v,
            "<=": lambda c, v: c <= v,
        }

        filtered = self.metadata_table
        for col_name, op, val in conditions:
            if col_name not in filtered.columns:
                self.error_occurred.emit(f"Column '{col_name}' not found in metadata.")
                return []

            if op not in allowed_ops:
                self.error_occurred.emit(f"Operator '{op}' not supported.")
                return []

            expr = allowed_ops[op](pl.col(col_name), val)
            filtered = filtered.filter(expr)

        results = []
        for row_dict in filtered.to_dicts():
            sample_id = row_dict.get("id")
            if sample_id is None:
                continue
            tga_data = self.read_entry(sample_id)
            results.append({
                "metadata": row_dict,
                "data": tga_data
            })
        return results


def create_dataset(path_to_directory):
    """
    High-level function to:
      1. Read local .txt TGA files (TXTDirectoryModel).
      2. Initialize a GoogleSpreadsheetModel and load the relevant worksheet.
      3. Create a TableModel and store TGA + metadata in separate Parquet files.
    """
    txtmodel = TXTDirectoryModel(path_to_directory)
    gspreadmodel = GoogleSpreadsheetModel()
    gspreadmodel._initialize_gspread()
    gspreadmodel.id_lookup_column = "TGA Identifier"
    gspreadmodel.load_worksheet("H2Lab_D2V_24_9 Melting Behaviour")

    model = TableModel(path_to_directory)
    model.create(gspreadmodel, txtmodel)

    # Example usage, reading a specific sample:
    df_rt12 = model.read_entry("RT12")
    if df_rt12 is not None:
        print("RT12 TGA data:\n", df_rt12)

    md_rt12 = model.find_metadata("RT12")
    print("RT12 metadata:\n", md_rt12)


if __name__ == "__main__":
    path_to_tga_files = "/Users/manuelleuchtenmuller/Library/CloudStorage/OneDrive-HydrogenReductionLab/H2Lab Projects/H2Lab_D2V_24_9 Melting Behaviour/TGA"

    # Example of creating dataset:
    os.remove(path_to_tga_files+"/metadata.parquet")
    create_dataset(path_to_tga_files)

    # Re-instantiate model to query data:

    my_model = TableModel(path_to_tga_files)

    # Single-condition find (compare "=="):
    samples_single = my_model.find("Sample Condition", "Washed")
    print(f"Found {len(samples_single)} samples with Condition == 'Washed'")

    # Multi-condition find_all (logical AND with arbitrary operators):
    samples_multi = my_model.find_all([
        ("Sample Condition", "==", "Washed"),
        ("Sample", "==", "EAFD9")

    ])

    print(f"Found {len(samples_multi)} samples with Condition == 'Washed' and EAFD9")
    for sample in samples_multi:
        print("Metadata:", sample["metadata"])
        #print("TGA data:\n", sample["data"])