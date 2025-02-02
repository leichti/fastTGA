from fastTGA.models.google_spreadsheet_model import GoogleSpreadsheetModel
from fastTGA.models.tga_file import TGAFile
from fastTGA.models.txt_directory_model import TXTDirectoryModel

from PyQt6.QtCore import QObject, pyqtSignal, QSettings
import polars as pl
import os


class TGADatasetModel(QObject):
    message_signal = pyqtSignal(str)

    def __init__(self):
        super().__init__()
        self.settings = QSettings('HydrogenreductionLab', 'fastTGA')


        # Initialize data members
        self.metadata_file = None
        self.metadata_table = pl.DataFrame()

        # Load last used directory from settings
        self.path_to_input = self.settings.value('tga_dataset/input_directory', '')
        self.path_to_output = self.settings.value('tga_dataset/output_directory', '')

        if self.path_to_output:
            self._initialize_directory()

    def _initialize_directory(self):
        """Initialize directory and load metadata if available"""
        if not os.path.exists(self.path_to_output):
            os.makedirs(self.path_to_output, exist_ok=True)

        self.metadata_file = os.path.join(self.path_to_output, "metadata.parquet")

        if os.path.exists(self.metadata_file):
            self.metadata_table = pl.read_parquet(self.metadata_file)

    def set_input_path(self, path_to_directory):
        """Set new working directory and save to settings"""
        self.path_to_input = path_to_directory
        self.settings.setValue('tga_dataset/input_directory', path_to_directory)


    def set_output_path(self, path_to_directory):
        """Set new output directory and save to settings"""
        self.path_to_output = path_to_directory
        self.settings.setValue('tga_dataset/output_directory', path_to_directory)
        self._initialize_directory()

    def save_metadata(self):
        """Write the current in-memory metadata table to disk."""
        if not self.metadata_table.is_empty() and self.metadata_file:
            self.metadata_table.write_parquet(self.metadata_file)
            self.message_signal.emit(f"Metadata saved to {self.metadata_file}")

    def add_entry(self, tga_file: TGAFile, gspread_metadata, save: bool = True):
        """Add or update a TGA entry with associated metadata"""
        if not self.path_to_output:
            self.message_signal.emit("No directory set. Please set a directory first.")
            return

        sample_id = tga_file.id
        tga_df = tga_file.data

        # Merge metadata
        combined_metadata = {**gspread_metadata, **tga_file.metadata, "id": sample_id}
        new_row = pl.DataFrame([combined_metadata])

        # Save TGA data
        sample_parquet = os.path.join(self.path_to_output, f"sample_{sample_id}.parquet")
        if os.path.exists(sample_parquet):
            os.remove(sample_parquet)
        tga_df.write_parquet(sample_parquet, compression="gzip")

        # Update metadata table
        if not self.metadata_table.is_empty():
            self.metadata_table = self.metadata_table.filter(pl.col("id") != sample_id)
        self.metadata_table = pl.concat([self.metadata_table, new_row], rechunk=True)

        if save:
            self.save_metadata()

        self.message_signal.emit(f"Added/updated entry: {sample_id}")

    def read_entry(self, sample_id: str) -> pl.DataFrame | None:
        """Load TGA data for given sample"""
        if not self.path_to_input:
            self.message_signal.emit("No directory set")
            return None

        sample_parquet = os.path.join(self.path_to_input, f"sample_{sample_id}.parquet")
        if os.path.exists(sample_parquet):
            return pl.read_parquet(sample_parquet)
        else:
            self.message_signal.emit(f"Sample {sample_id} not found")
            return None

    def find_metadata(self, sample_id: str) -> pl.DataFrame:
        """Find metadata for given sample ID"""
        if self.metadata_table.is_empty():
            return pl.DataFrame()
        return self.metadata_table.filter(pl.col("id") == sample_id)

    def find(self, column_name: str, value, operator: str = "==") -> list[dict]:
        """Single condition search"""
        ops = {
            "==": lambda c, v: c == v,
            "!=": lambda c, v: c != v,
            ">": lambda c, v: c > v,
            "<": lambda c, v: c < v,
            ">=": lambda c, v: c >= v,
            "<=": lambda c, v: c <= v,
        }

        if operator not in ops:
            self.message_signal.emit(f"Operator '{operator}' not supported. Using '=='.")
            operator = "=="

        if column_name not in self.metadata_table.columns:
            self.message_signal.emit(f"Column '{column_name}' not found in metadata.")
            return []

        filtered = self.metadata_table.filter(ops[operator](pl.col(column_name), value))
        return [self._create_result_dict(row) for row in filtered.to_dicts()]

    def find_all(self, conditions: list[tuple[str, str, object]]) -> list[dict]:
        """Multi-condition search with AND logic"""
        ops = {
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
                self.message_signal.emit(f"Column '{col_name}' not found in metadata.")
                return []
            if op not in ops:
                self.message_signal.emit(f"Operator '{op}' not supported.")
                return []
            filtered = filtered.filter(ops[op](pl.col(col_name), val))

        return [self._create_result_dict(row) for row in filtered.to_dicts()]

    def _create_result_dict(self, row_dict: dict) -> dict:
        """Helper method to create result dictionary with metadata and data"""
        sample_id = row_dict.get("id")
        if not sample_id:
            return {}
        return {
            "metadata": row_dict,
            "data": self.read_entry(sample_id)
        }



def create_dataset(path_to_directory):
    txtmodel = TXTDirectoryModel(path_to_directory)
    gspreadmodel = GoogleSpreadsheetModel()
    gspreadmodel._initialize_gspread()
    gspreadmodel.lookup_column = "TGA Identifier"
    gspreadmodel.load_worksheet("H2Lab_D2V_24_9 Melting Behaviour")

    model = TGADatasetModel()

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

    my_model = TGADatasetModel(path_to_tga_files)

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