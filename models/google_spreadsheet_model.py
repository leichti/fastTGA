import asyncio
from concurrent.futures import ThreadPoolExecutor

import gspread
import polars as pl
from PyQt6.QtCore import QObject, pyqtSignal, QThread


class GoogleSpreadsheetModel(QThread):
    initialized = pyqtSignal()
    worksheet_loaded = pyqtSignal(pl.DataFrame)
    error_occurred = pyqtSignal(str)

    def __init__(self, path_to_credentials=None):
        super().__init__()
        self.table_df = None
        self.columns = None
        self.available_worksheets = []
        self.worksheet_to_load = None
        self.id_lookup_column = ""
        self.path_to_credentials = path_to_credentials

    def set_json_credentials(self, json_credentials):
        self.path_to_credentials = json_credentials

    def run(self):
        try:
            self._initialize_gspread()
            self.initialized.emit()
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
            self.columns = self.table_df.columns

            self.worksheet_loaded.emit(self.table_df)
        except Exception as e:
            self.table_df = pl.DataFrame([])
            self.columns = []
            self.error_occurred.emit(f"Error loading worksheet '{name}': {e}")

    def get_metadata(self, id):
        if self.table_df is None:
            return None
        if self.id_lookup_column:
            filtered_df = self.table_df.filter(pl.col(self.id_lookup_column) == id)
            if len(filtered_df) == 1:
                return filtered_df.to_dicts()[0]
            self.error_occurred.emit(f"Found no or multiple entries for id: {id} in column: {self.id_lookup_column}")
        return None

