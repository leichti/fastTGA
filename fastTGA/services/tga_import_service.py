from fastTGA.models.google_spreadsheet_model import GoogleSpreadsheetModel
from fastTGA.models.tga_dataset_model import TGADatasetModel
from fastTGA.models.txt_directory_model import TXTDirectoryModel
from fastTGA.services.tga_entry_preparator import TGAEntryPreparator


class TGAImportService:
    def __init__(self, dataset_model: TGADatasetModel, entry_preparator: TGAEntryPreparator):
        self.dataset_model = dataset_model
        self.entry_preparator = entry_preparator

    def import_from_txt_directory(self, gspread_model: GoogleSpreadsheetModel,
                                txtmodel: TXTDirectoryModel):
        """Moved from TGADatasetModel.create"""
        for file_info in txtmodel.txt_files:
            tga_file, metadata = self.entry_preparator.prepare_entry_data(
                file_info, gspread_model
            )
            if tga_file is not None and metadata is not None:
                self.dataset_model.add_entry(tga_file, metadata, save=True)