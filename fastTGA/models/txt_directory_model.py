import os
import re

from PyQt6.QtCore import QObject, pyqtSignal


class TXTDirectoryModel(QObject):
    txt_files_loaded = pyqtSignal(list)

    def __init__(self, txt_directory=None):
        super().__init__()
        self.txt_directory = txt_directory
        self.regex = r"RT[0-9]{1,2}"
        self.txt_files = []
        if txt_directory:
            self.load_txt_files()

    def set_path(self, txt_directory):
        self.txt_directory = txt_directory
        self.load_txt_files()

    def set_file_filter(self, regex):
        self.regex = regex
        self.load_txt_files()

    def get_txt_directory(self):
        return self.txt_directory

    def load_txt_files(self):
        """
        Loads .txt files from a directory, extracting an ID from the filename based on a regex pattern.
        """
        self.txt_files = []  # Initialize the list of .txt files
        if self.txt_directory:
            # Iterate over .txt files in the directory
            for file in os.listdir(self.txt_directory):
                if file.endswith('.txt'):
                    file_id = ""  # Default file ID if no regex match
                    if self.regex:
                        # Ensure regex is valid and matches the filename
                        match = re.search(self.regex, file)
                        if match:  # Check if a match is found
                            file_id = match.group(0)  # Extract the matched group

                    # Append the file path and extracted ID to the list
                    self.txt_files.append({"path": os.path.join(self.txt_directory, file), "id": file_id})

        # Emit the loaded files
        self.txt_files_loaded.emit(self.txt_files)