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
            self.error_occurred.emit(f"Worksheet '{name}' not found in available worksheets.")
            return

        try:
            worksheet = self.spreadsheet.worksheet(name)
            data = worksheet.get_all_records()
            self.table_df = pl.DataFrame(data)
            self.table_df = self.table_df.rename({col: col.replace("\n", "") for col in self.table_df.columns})

            # Store settings
            self.worksheet_to_load = name
            self.settings.setValue('google_spreadsheet_model/last_worksheet', name)

            # Set first column as lookup column if not set
            if self.table_df.columns:
                self.set_lookup_column(self.table_df.columns[0])

            self.worksheet_loaded.emit(self.table_df)

        except Exception as e:
            self.table_df = pl.DataFrame([])
            self.error_occurred.emit(f"Error loading worksheet '{name}': {e}")

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