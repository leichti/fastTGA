from fastTGA.models.google_spreadsheet_model import GoogleSpreadsheetModel
from fastTGA.models.tga_file import TGAFile


class TGAEntryPreparator:

    def __init__(self, config):
        self.config = config or {}


    def prepare_entry_data(self, tga_file_dict, gspread_model: GoogleSpreadsheetModel):
        """
        1. Create a TGAFile instance from the path in tga_file_dict.
        2. Look up the metadata from gspread_model using either 'id' or 'name'.
        3. Return (tga_file, metadata) or signal an error if not found.
        """
        file_id = tga_file_dict["id"]
        file_path = tga_file_dict["path"]

        tga_file = TGAFile(file_path)  # Loads Polars DataFrame in tga_file.data

        downsample_frequency = self.config.get("downsample_frequency", None)
        if downsample_frequency:
            tga_file.downsample(downsample_frequency)

        if self.config.get("calculate_dm_dt", False):
            tga_file.calculate_dm_dt_in_s()

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