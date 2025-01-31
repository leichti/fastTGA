import asyncio
import sys

from PyQt6 import QtWidgets
from PyQt6.QtCore import QEventLoop
from PyQt6.QtWidgets import QApplication

from models.google_spreadsheet_model import GoogleSpreadsheetModel
from models.txt_directory_model import TXTDirectoryModel
from viewmodels.data_widget_view_model import DataWidgetViewModel
from views.main_window import MainWindow
from views.data_widget import DataWidget

def main():
    app = QtWidgets.QApplication.instance()
    if app is None:
        app = QtWidgets.QApplication(sys.argv)

    # models
    txt_directory_model = TXTDirectoryModel()
    gspread_model = GoogleSpreadsheetModel()
    gspread_model.start()

    # view models
    data_widget_view_model = DataWidgetViewModel(txt_directory_model,
                                                 gspread_model)

    # views
    data_widget = DataWidget(data_widget_view_model)

    window = MainWindow(data_widget)
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()