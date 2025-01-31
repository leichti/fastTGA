import sys
from PyQt6.QtWidgets import (
    QApplication,
    QMainWindow,
    QWidget,
    QHBoxLayout,
    QTabWidget,
)

from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure


class MainWindow(QMainWindow):
    def __init__(self, data_widget):
        super().__init__()

        self.setWindowTitle("PyQt6 Matplotlib App")

        # Create the main layout (horizontal)
        main_layout = QHBoxLayout()

        # --- Left side: QTabWidget ---
        self.tab_widget = QTabWidget()
        self.tab_widget.addTab(data_widget, "Import Data")
        main_layout.addWidget(self.tab_widget)

        # --- Right side: Matplotlib Canvas ---
        self.figure = Figure()
        self.canvas = FigureCanvas(self.figure)
        main_layout.addWidget(self.canvas)

        # --- Set the main layout to a central widget ---
        central_widget = QWidget()
        central_widget.setLayout(main_layout)
        self.setCentralWidget(central_widget)
