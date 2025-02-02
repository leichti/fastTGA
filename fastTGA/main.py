import json
import os
import sys

from PyQt6 import QtWidgets

from fastTGA.models.google_spreadsheet_model import GoogleSpreadsheetModel
from fastTGA.models.tga_dataset_model import TGADatasetModel
from fastTGA.models.tga_tableview_model import TGATableviewModel
from fastTGA.models.txt_directory_model import TXTDirectoryModel
from fastTGA.viewmodels.data_widget_view_model import DataWidgetViewModel
from fastTGA.views.data_overview_table_widget import DataOverviewTableWidget
from fastTGA.views.main_window import MainWindow
from fastTGA.views.data_widget import DataWidget
from fastTGA.views.data_preparation_widget import DataPreparationWidget


def load_config(config_path='config.json'):
    if os.path.exists(config_path):
        with open(config_path, 'r') as file:
            return json.load(file)
    else:
        return {}

def main():
    app = QtWidgets.QApplication.instance()
    if app is None:
        app = QtWidgets.QApplication(sys.argv)

    # models
    txt_directory_model = TXTDirectoryModel()
    gspread_model = GoogleSpreadsheetModel()
    gspread_model.start()
    tga_dataset_model = TGADatasetModel()
    tga_tableview_model = TGATableviewModel(tga_dataset_model)


    # view models
    data_widget_view_model = DataWidgetViewModel(txt_directory_model,
                                                 gspread_model,
                                                 tga_dataset_model)

    # views
    data_widget = DataWidget(data_widget_view_model)
    data_preparation_widget = DataPreparationWidget(data_widget_view_model)
    data_overview_table_widget = DataOverviewTableWidget(tga_tableview_model)

    window = MainWindow(data_widget,
                        data_preparation_widget,
                        data_overview_table_widget)
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()