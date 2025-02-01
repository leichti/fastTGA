from PyQt6.QtWidgets import (
    QMainWindow,
    QWidget,
    QHBoxLayout,
    QTabWidget,
)

from fastTGA.views.data_overview_table_widget import DataOverviewTableWidget
from fastTGA.views.data_preparation_widget import DataPreparationWidget
from fastTGA.views.data_widget import DataWidget


class MainWindow(QMainWindow):
    def __init__(self,
                 data_widget : DataWidget,
                 data_preparation_widget : DataPreparationWidget,
                 data_view_table_widget : DataOverviewTableWidget):
        super().__init__()

        self.setWindowTitle("FastTGA Data Transformer")

        # Create the main layout (horizontal)
        main_layout = QHBoxLayout()

        # --- Left side: QTabWidget ---
        self.tab_widget = QTabWidget()
        self.tab_widget.addTab(data_widget, "Import Data")
        self.tab_widget.addTab(data_preparation_widget, "Data Preparation")
        main_layout.addWidget(self.tab_widget)


        # --- Right side: Matplotlib Canvas ---
        main_layout.addWidget(data_view_table_widget)


        # --- Set the main layout to a central widget ---
        central_widget = QWidget()
        central_widget.setLayout(main_layout)
        self.setCentralWidget(central_widget)
