from PyQt6 import QtWidgets
from PyQt6.QtWidgets import QWidget
from ui.data_widget_ui import Ui_DataWidget
from viewmodels.data_widget_view_model import DataWidgetViewModel


class DataWidget(QWidget, Ui_DataWidget):
    def __init__(self, data_widget_view_model:DataWidgetViewModel, parent=None):
        super(DataWidget, self).__init__(parent)
        self.setupUi(self)

        self.data_widget_view_model = data_widget_view_model

        self.data_widget_view_model.txt_files_found.connect(self.update_txt_directory_info)
        self.data_widget_view_model.worksheet_list_updated.connect(self.initialize_google_sheetname_combobox)
        self.data_widget_view_model.available_columns_updated.connect(self.initliaze_google_lookup_column)


        # Connect signals
        self.open_txt_directory_pushButton.clicked.connect(self.open_txt_directory)
        self.filename_regex_lineEdit.textChanged.connect(self.data_widget_view_model.set_regex)
        #self.google_sheetnames_comboBox.currentTextChanged.connect(self.update_google_sheetname)
        self.google_sheetnames_comboBox.currentTextChanged.connect(self.set_google_lookup_column)
        self.filename_regex_lineEdit.setText(r"RT[0-9]{1,2}")


    def open_txt_directory(self):
        # get directory handler
        directory = QtWidgets.QFileDialog.getExistingDirectory(self, "Select Directory")
        self.data_widget_view_model.set_txt_directory(directory)

    def update_txt_directory_info(self, example_filename, example_id, files_found):
        self.example_filename_id_lineEdit.setText(example_id)
        self.example_filename_lineEdit.setText(example_filename)
        self.files_in_directory_lineEdit.setText(str(files_found))

    def initialize_google_sheetname_combobox(self, sheetnames):
        # block signals:
        self.google_sheetnames_comboBox.blockSignals(True)
        # clear all old items
        self.google_sheetnames_comboBox.clear()
        self.google_sheetnames_comboBox.addItems(sheetnames)
        self.google_sheetnames_comboBox.setCurrentIndex(0)
        self.data_widget_view_model.select_sheet_from_gspread(sheetnames[0])
        # unblock signals:
        self.google_sheetnames_comboBox.blockSignals(False)

    def initliaze_google_lookup_column(self, columns):
        # block signals:
        self.google_lookup_column_comboBox.blockSignals(True)
        # clear all old items
        self.google_lookup_column_comboBox.clear()
        self.google_lookup_column_comboBox.addItems(columns)
        self.google_lookup_column_comboBox.setCurrentIndex(0)
        # unblock signals:
        self.google_lookup_column_comboBox.blockSignals(False)

    def set_google_lookup_column(self, column):
        self.data_widget_view_model.select_sheet_from_gspread(column)