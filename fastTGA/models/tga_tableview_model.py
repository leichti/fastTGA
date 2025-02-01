from typing import Any

from PyQt6.QtCore import QAbstractTableModel, QModelIndex, Qt

from fastTGA.models.tga_dataset_model import TGADatasetModel


class TGATableviewModel(QAbstractTableModel):
    def __init__(self, dataset_model: TGADatasetModel):
        super().__init__()
        self.dataset_model = dataset_model
        self._data = dataset_model.metadata_table
        self._headers = self._data.columns if not self._data.is_empty() else []

    def rowCount(self, parent=QModelIndex()) -> int:
        return len(self._data) if not self._data.is_empty() else 0

    def columnCount(self, parent=QModelIndex()) -> int:
        return len(self._headers)

    def data(self, index: QModelIndex, role=Qt.ItemDataRole.DisplayRole) -> Any:
        if not index.isValid():
            return None

        if role == Qt.ItemDataRole.DisplayRole:
            value = self._data[index.row(), index.column()]
            return str(value)

        return None

    def headerData(self, section: int, orientation: Qt.Orientation, role=Qt.ItemDataRole.DisplayRole) -> Any:
        if role == Qt.ItemDataRole.DisplayRole:
            if orientation == Qt.Orientation.Horizontal:
                return str(self._headers[section])
            else:
                return str(section + 1)
        return None

    def refresh_data(self):
        """Refresh the view when dataset model changes"""
        self.beginResetModel()
        self._data = self.dataset_model.metadata_table
        self._headers = self._data.columns if not self._data.is_empty() else []
        self.endResetModel()

    def get_row_data(self, row: int) -> dict:
        """Get all data for a specific row"""
        if self._data.is_empty() or row >= len(self._data):
            return {}
        return self._data[row].to_dicts()[0]

    def get_sample_id(self, row: int) -> str | None:
        """Get sample ID for a specific row"""
        row_data = self.get_row_data(row)
        return row_data.get('id')

    def flags(self, index: QModelIndex) -> Qt.ItemFlag:
        return Qt.ItemFlag.ItemIsEnabled | Qt.ItemFlag.ItemIsSelectable