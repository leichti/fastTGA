import os
import gspread
import polars as pl
from PyQt6.QtCore import QThread, pyqtSignal, QSettings


class GoogleSpreadsheetModel(QThread):
    initialized = pyqtSignal(list, str, list, str)
    worksheet_loaded = pyqtSignal(pl.DataFrame)
    error_occurred = pyqtSignal(str)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.settings = QSettings('HydrogenreductionLab', 'fastTGA')

        # Data members
        self.client = None
        self.spreadsheet = None
        self.table_df = None
        self.available_worksheets = []

        # Load settings
        self.path_to_credentials = self.settings.value('google_spreadsheet_model/credentials_path', None)
        self.worksheet_to_load = self.settings.value('google_spreadsheet_model/last_worksheet', None)
        self.lookup_column = self.settings.value('google_spreadsheet_model/lookup_column', '')

        if self.path_to_credentials:
            self.start()

    def set_json_credentials(self, file_path_to_json_credentials):
        if os.path.exists(file_path_to_json_credentials):
            self.path_to_credentials = file_path_to_json_credentials
            self.settings.setValue('google_spreadsheet_model/credentials_path', self.path_to_credentials)
            self.start()
        else:
            self.error_occurred.emit(f"File not found: {file_path_to_json_credentials}")

    def get_available_columns(self):
        return self.table_df.columns if self.table_df is not None else []

    def run(self):
        try:
            self._initialize_gspread()

            self.initialized.emit(self.available_worksheets,
                                  self.worksheet_to_load,
                                  self.get_available_columns(),
                                  self.lookup_column)

            # if there is a lust worksheet saved in settings, load it
            if self.worksheet_to_load:
                self.load_worksheet(self.worksheet_to_load)

        except Exception as e:
            print("error", e)
            self.error_occurred.emit(str(e))

    def _initialize_gspread(self):
        self.client = gspread.service_account(self.path_to_credentials)
        self.spreadsheet = self.client.open_by_key("1HooNjAziwRFESXFmY-s6S8lxb8Ztt_YAEoxR19NaE-Q")
        self.available_worksheets = [worksheet.title for worksheet in self.spreadsheet.worksheets()]

    def load_worksheet(self, name):
        if name not in self.available_worksheets:
            error_msg = f"Worksheet '{name}' not found in available worksheets."
            print(error_msg)
            self.error_occurred.emit(error_msg)
            return

        print(f"Attempting to load worksheet: '{name}'")
        data = []  # Initialize to prevent UnboundLocalError in except block

        try:
            worksheet = self.spreadsheet.worksheet(name)
            data = worksheet.get_all_records()  # Use head=1

            # === Handle Empty Data / No Headers Gracefully ===
            if not data:
                print(f"Worksheet '{name}' appears empty or has no data after the header.")
                self.table_df = pl.DataFrame()  # Assign empty DataFrame
                # Decide if you want to emit an error or just load empty:
                # self.error_occurred.emit(f"Worksheet '{name}' is empty or has no data.")
            else:
                all_string_schema = {}
                # Check if the first row (headers dict) is non-empty
                if data[0]:
                    all_string_schema = {
                        str(col).replace("\n", "").strip(): pl.Utf8
                        for col in data[0].keys()
                    }

                if not all_string_schema:
                    print(f"No headers found or first row is empty in worksheet '{name}'.")
                    self.table_df = pl.DataFrame()  # Assign empty DataFrame
                    # Decide if you want to emit an error or just load empty:
                    # self.error_occurred.emit(f"No headers found in worksheet '{name}'.")
                else:
                    # === Load using schema_overrides ===
                    print(f"Loading '{name}' with columns as Utf8: {list(all_string_schema.keys())}")
                    self.table_df = pl.DataFrame(data, schema_overrides=all_string_schema)

                    # === Verify/Fix Column Names (More Robust than blind rename) ===
                    current_cols = self.table_df.columns
                    rename_map = {
                        curr: clean
                        for curr, clean in zip(current_cols, all_string_schema.keys())
                        if curr != clean
                    }
                    if rename_map:
                        print(f"Renaming columns for consistency: {rename_map}")
                        self.table_df = self.table_df.rename(rename_map)

            # === Continue only if DataFrame was created successfully ===
            if self.table_df is not None:  # Check if df exists (might be empty)
                # Store settings
                self.worksheet_to_load = name
                self.settings.setValue('google_spreadsheet_model/last_worksheet', name)

                # Set lookup column (more robustly)
                if not self.table_df.is_empty() and self.table_df.columns:
                    # Try to restore previous setting, otherwise default to first col
                    previous_lookup = self.settings.value('google_spreadsheet_model/last_lookup_column')
                    if previous_lookup and previous_lookup in self.table_df.columns:
                        self.set_lookup_column(previous_lookup)
                    else:
                        if previous_lookup:
                            print("Warning")
                        self.set_lookup_column(self.table_df.columns[0])

                self.worksheet_loaded.emit(self.table_df)
                print(f"Worksheet '{name}' loaded successfully.")

        except Exception as e:
            error_msg = f"Error loading worksheet '{name}': {e}"
            self.table_df = pl.DataFrame([])  # Ensure table_df is an empty DataFrame on error
            self.error_occurred.emit(error_msg)  # Emit only the error, not 'data'
            # Emit empty df to clear view
            self.worksheet_loaded.emit(self.table_df)

    def set_lookup_column(self, column):
        self.lookup_column = column
        self.settings.setValue('google_spreadsheet_model/lookup_column', column)

    def get_metadata(self, id):
        if self.table_df is None:
            return None
        if self.lookup_column:
            filtered_df = self.table_df.filter(pl.col(self.lookup_column) == id)
            if len(filtered_df) == 1:
                return filtered_df.to_dicts()[0]
            self.error_occurred.emit(f"Found no or multiple entries for id: {id} in column: {self.lookup_column}")
        return None

    def get_first_id(self):
        if self.table_df is None:
            return None
        if self.lookup_column and len(self.table_df) > 0:
            return self.table_df[self.lookup_column][0]
        return None