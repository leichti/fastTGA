from PyQt6.QtCore import QObject, pyqtSignal

from models.google_spreadsheet_model import GoogleSpreadsheetModel
from models.txt_directory_model import TXTDirectoryModel


class DataWidgetViewModel(QObject):
    txt_files_found = pyqtSignal(str,str,int)
    worksheet_list_updated = pyqtSignal(list)
    available_columns_updated = pyqtSignal(list)

    def __init__(self,
                 txt_directory_model: TXTDirectoryModel,
                 gspread_model:GoogleSpreadsheetModel):
        # call super class constructor
        super().__init__()
        self.txt_directory_model = txt_directory_model
        self.gspread_model = gspread_model
        self.gspread_model.initialized.connect(self.gspread_initialized)
        self.gspread_model.worksheet_loaded.connect(self.worksheet_data_available)
        self.gspread_model.error_occurred.connect(print)

        self.txt_directory_model.txt_files_loaded.connect(self.txt_files_loaded)

    def set_txt_directory(self, directory):
        self.txt_directory_model.set_txt_directory(directory)
        self.txt_directory_model.load_txt_files()

    def txt_files_loaded(self, txt_files):
        if len(txt_files) == 0:
            return False

        first_row = txt_files[0] if txt_files else None
        example_filename, example_id = first_row["path"], first_row["id"] if first_row else None
        files_found = len(txt_files)

        self.txt_files_found.emit(example_filename, example_id, files_found)

    def set_regex(self, regex):
        self.txt_directory_model.set_file_filter(regex)

    def gspread_initialized(self):
        available_sheets = self.gspread_model.available_worksheets
        self.worksheet_list_updated.emit(available_sheets)

    def select_sheet_from_gspread(self, sheetname):
        self.gspread_model.load_worksheet(sheetname)

    def worksheet_data_available(self, table_df):
        columns = table_df.columns
        self.available_columns_updated.emit(columns)

