from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import QWidget
from PyQt6.QtGui import QDoubleValidator

from fastTGA.ui.tga_data_preparation_widget_ui import Ui_TGADataPreparationWidget
from fastTGA.viewmodels.data_widget_view_model import DataWidgetViewModel


class DataPreparationWidget(QWidget, Ui_TGADataPreparationWidget):
    """
    A widget for TGA data preparation that binds UI elements to actions on the data widget view model.
    """

    def __init__(self, data_widget_view_model: DataWidgetViewModel, parent=None):
        """
        Initialize the TGA data preparation widget.

        Args:
            data_widget_view_model (DataWidgetViewModel): The view model to update.
            parent: The parent widget.
        """
        super().__init__(parent)
        self.setupUi(self)

        self.data_widget_view_model = data_widget_view_model

        # Set a double validator for the sample frequency input (adjust min, max, and decimals as required)
        validator = QDoubleValidator(0.0, 1e10, 2, self)
        self.sample_frequency_lineEdit.setValidator(validator)

        # Connect signals to slots
        self.dmdt_checkBox.stateChanged.connect(self.on_dmdt_checkbox_state_changed)
        self.sample_frequency_lineEdit.textChanged.connect(self.on_sample_frequency_text_changed)

    def on_dmdt_checkbox_state_changed(self, state: int) -> None:
        """
        Handle changes to the dmdt checkbox state.

        Args:
            state (int): The new state of the checkbox.
        """
        # Converts state to a boolean; assuming Qt.CheckState.Checked equals True.
        boolean_state = (state == 2)
        self.data_widget_view_model.set_dmdt_checkbox_state(boolean_state)

    def on_sample_frequency_text_changed(self, frequency: str) -> None:
        """
        Handle the textChanged event for the sample frequency line edit. Converts the input text to float.

        Args:
            frequency (str): The frequency input as text.
        """
        try:
            frequency_value = float(frequency)
            self.data_widget_view_model.set_sample_frequency(frequency_value)
        except ValueError:
            # Log error or notify the user; for now, we simply print the error.
            print(f"Invalid frequency entered: {frequency}")