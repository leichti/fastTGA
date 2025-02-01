from PyQt6.QtWidgets import QWidget

from fastTGA.ui.data_overview_table_widget_ui import Ui_DataOverviewTableWidget


class DataOverviewTableWidget(QWidget, Ui_DataOverviewTableWidget):
    def __init__(self, tga_tableview_model, parent=None):
        super(DataOverviewTableWidget, self).__init__(parent)
        self.setupUi(self)
        self.data = None
        self.tableView.setModel(tga_tableview_model)

    def set_data(self, data):
        self.data = data

