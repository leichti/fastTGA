from PyQt6 import QtWidgets
from PyQt6.QtWidgets import QWidget
from fastTGA.ui.data_widget_ui import Ui_DataWidget
from fastTGA.viewmodels.data_widget_view_model import DataWidgetViewModel


class DataWidget(QWidget, Ui_DataWidget):
    def __init__(self, data_widget_view_model:DataWidgetViewModel, parent=None):
        super(DataWidget, self).__init__(parent)
        self.setupUi(self)

        self.data_widget_view_model = data_widget_view_model

        self.data_widget_view_model.txt_files_found.connect(self.update_txt_directory_info)
        self.data_widget_view_model.worksheet_list_updated.connect(self.initialize_google_sheetname_combobox)
        self.data_widget_view_model.available_columns_updated.connect(self.initliaze_google_lookup_column)
        self.data_widget_view_model.print_message.connect(self.print_message)
        self.data_widget_view_model.new_example_id_available.connect(self.update_gspread_example_id)


        # Connect signals
        self.open_txt_directory_pushButton.clicked.connect(self.open_txt_directory)
        self.filename_regex_lineEdit.textChanged.connect(self.data_widget_view_model.set_regex)
        self.google_sheetnames_comboBox.currentTextChanged.connect(self.set_gspread_sheet)
        self.google_lookup_column_comboBox.currentTextChanged.connect(self.data_widget_view_model.set_gspread_lookup_column)
        self.select_api_pushButton.clicked.connect(self.open_api_selection)
        self.generate_hdf5_pushButton.clicked.connect(self.data_widget_view_model.create_dataset)
        self.select_output_directory_pushButton.clicked.connect(self.open_directory_dialog_for_output)
        self.filename_regex_lineEdit.setText(r"RT[0-9]{1,2}")

    def open_txt_directory(self):
        # get directory handler
        directory = QtWidgets.QFileDialog.getExistingDirectory(self, "Select Directory")
        self.data_widget_view_model.set_txt_directory(directory)

    def update_txt_directory_info(self, example_filename, example_id, files_found):
        self.example_filename_id_lineEdit.setText(example_id)
        self.example_filename_lineEdit.setText(example_filename)
        self.files_in_directory_lineEdit.setText(str(files_found))

    def initialize_google_sheetname_combobox(self, sheetnames, select_id):
        # block signals:
        self.google_sheetnames_comboBox.blockSignals(True)
        # clear all old items
        self.google_sheetnames_comboBox.clear()
        self.google_sheetnames_comboBox.addItems(sheetnames)
        self.google_sheetnames_comboBox.setCurrentIndex(select_id)
        self.data_widget_view_model.set_gspread_sheet(sheetnames[0])
        # unblock signals:
        self.google_sheetnames_comboBox.blockSignals(False)

    def initliaze_google_lookup_column(self, columns, select_id):
        # block signals:
        self.google_lookup_column_comboBox.blockSignals(True)
        # clear all old items
        self.google_lookup_column_comboBox.clear()
        self.google_lookup_column_comboBox.addItems(columns)
        self.google_lookup_column_comboBox.setCurrentIndex(select_id)
        # unblock signals:
        self.google_lookup_column_comboBox.blockSignals(False)

    def set_gspread_sheet(self, column):
        self.data_widget_view_model.set_gspread_sheet(column)

    def open_api_selection(self):
        directory_path, _ = QtWidgets.QFileDialog.getOpenFileName(self, "Select API JSON file")
        self.data_widget_view_model.set_api_json(directory_path)

    def print_message(self, message):
        self.logging_plainTextEdit.appendPlainText(message)

    def update_gspread_example_id(self, example_id):
        self.google_lookup_id_lineEdit.setText(example_id)

    def open_directory_dialog_for_output(self):
        directory = QtWidgets.QFileDialog.getExistingDirectory(self, "Select Directory")
        self.data_widget_view_model.select_output_directory(directory)