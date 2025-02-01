# Form implementation generated from reading ui file './tga_data_preparation_widget.ui'
#
# Created by: PyQt6 UI code generator 6.8.0
#
# WARNING: Any manual changes made to this file will be lost when pyuic6 is
# run again.  Do not edit this file unless you know what you are doing.


from PyQt6 import QtCore, QtGui, QtWidgets


class Ui_TGADataPreparationWidget(object):
    def setupUi(self, TGADataPreparationWidget):
        TGADataPreparationWidget.setObjectName("TGADataPreparationWidget")
        TGADataPreparationWidget.resize(298, 453)
        self.verticalLayout = QtWidgets.QVBoxLayout(TGADataPreparationWidget)
        self.verticalLayout.setObjectName("verticalLayout")
        self.horizontalLayout = QtWidgets.QHBoxLayout()
        self.horizontalLayout.setObjectName("horizontalLayout")
        self.label = QtWidgets.QLabel(parent=TGADataPreparationWidget)
        self.label.setObjectName("label")
        self.horizontalLayout.addWidget(self.label)
        self.sample_frequency_lineEdit = QtWidgets.QLineEdit(parent=TGADataPreparationWidget)
        self.sample_frequency_lineEdit.setObjectName("sample_frequency_lineEdit")
        self.horizontalLayout.addWidget(self.sample_frequency_lineEdit)
        self.horizontalLayout.setStretch(0, 3)
        self.horizontalLayout.setStretch(1, 2)
        self.verticalLayout.addLayout(self.horizontalLayout)
        self.dmdt_checkBox = QtWidgets.QCheckBox(parent=TGADataPreparationWidget)
        self.dmdt_checkBox.setObjectName("dmdt_checkBox")
        self.verticalLayout.addWidget(self.dmdt_checkBox)
        spacerItem = QtWidgets.QSpacerItem(20, 40, QtWidgets.QSizePolicy.Policy.Minimum, QtWidgets.QSizePolicy.Policy.Expanding)
        self.verticalLayout.addItem(spacerItem)

        self.retranslateUi(TGADataPreparationWidget)
        QtCore.QMetaObject.connectSlotsByName(TGADataPreparationWidget)

    def retranslateUi(self, TGADataPreparationWidget):
        _translate = QtCore.QCoreApplication.translate
        TGADataPreparationWidget.setWindowTitle(_translate("TGADataPreparationWidget", "Form"))
        self.label.setText(_translate("TGADataPreparationWidget", "Sampling Frequency in s"))
        self.dmdt_checkBox.setText(_translate("TGADataPreparationWidget", "Calculate dmdt"))


if __name__ == "__main__":
    import sys
    app = QtWidgets.QApplication(sys.argv)
    TGADataPreparationWidget = QtWidgets.QWidget()
    ui = Ui_TGADataPreparationWidget()
    ui.setupUi(TGADataPreparationWidget)
    TGADataPreparationWidget.show()
    sys.exit(app.exec())
