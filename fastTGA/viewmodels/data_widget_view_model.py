from PyQt6.QtCore import QObject, pyqtSignal

from fastTGA.models.google_spreadsheet_model import GoogleSpreadsheetModel
from fastTGA.models.tga_dataset_model import TGADatasetModel
from fastTGA.models.txt_directory_model import TXTDirectoryModel
from fastTGA.services.tga_entry_preparator import TGAEntryPreparator
from fastTGA.services.tga_import_service import TGAImportService


class DataWidgetViewModel(QObject):
    txt_files_found = pyqtSignal(str,str,int)
    worksheet_list_updated = pyqtSignal(list, int)
    available_columns_updated = pyqtSignal(list, int)
    print_message = pyqtSignal(str)
    new_example_id_available = pyqtSignal(str)

    def __init__(self,
                 txt_directory_model: TXTDirectoryModel,
                 gspread_model:GoogleSpreadsheetModel,
                 tga_dataset_model:TGADatasetModel):
        # call super class constructor
        super().__init__()
        self.txt_directory_model = txt_directory_model
        self.gspread_model = gspread_model
        self.tga_dataset_model = tga_dataset_model

        self.tga_data_entry_preparator = TGAEntryPreparator({"calculate_dm_dt":False,
                                                       "downsample_frequency":None})

        self.gspread_model.initialized.connect(self.gspread_initialized)
        self.gspread_model.worksheet_loaded.connect(self.worksheet_data_available)
        self.gspread_model.error_occurred.connect(self.send_print_message)

        self.txt_directory_model.txt_files_loaded.connect(self.txt_files_loaded)

    def set_txt_directory(self, directory):
        self.txt_directory_model.set_path(directory)
        self.tga_dataset_model.set_input_path(directory)
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

    def gspread_initialized(self, available_sheets, last_sheet, available_columns, lookup_column):
        # get id of last sheet in available_sheets list
        last_sheet_id = available_sheets.index(last_sheet) if last_sheet in available_sheets else 0
        self.worksheet_list_updated.emit(available_sheets, last_sheet_id)

        # get id of lookup_column in available_columns list
        lookup_column_id = available_columns.index(lookup_column) if lookup_column in available_columns else 0
        self.available_columns_updated.emit(available_columns, lookup_column_id)

    def set_gspread_sheet(self, sheetname):
        self.gspread_model.load_worksheet(sheetname)

    def worksheet_data_available(self, table_df):
        columns = table_df.columns
        self.available_columns_updated.emit(columns, 0)
        example_id = self.gspread_model.get_first_id()
        self.new_example_id_available.emit(example_id)
        self.print_message.emit(f"Worksheet loaded with {len(table_df)} rows and {len(columns)} columns")

    def set_api_json(self, file_path):
        self.gspread_model.set_json_credentials(file_path)

    def send_print_message(self, message):
        self.print_message.emit(message)

    def set_gspread_lookup_column(self, column):
        self.send_print_message(f"Lookup column set to {column}")
        self.gspread_model.set_lookup_column(column)
        example_id = self.gspread_model.get_first_id()
        self.new_example_id_available.emit(str(example_id))

    def set_dmdt_checkbox_state(self, state):
        self.tga_data_entry_preparator.config["calculate_dm_dt"] = state

    def create_dataset(self):
        tga_import_service = TGAImportService(self.tga_dataset_model, self.tga_data_entry_preparator)
        tga_import_service.import_from_txt_directory(self.gspread_model, self.txt_directory_model)

    def set_sample_frequency(self, frequency):
        self.tga_data_entry_preparator.config["downsample_frequency"] = frequency

    def select_output_directory(self, directory):
        self.tga_dataset_model.set_output_path(directory)

