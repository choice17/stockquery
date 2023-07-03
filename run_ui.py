import os
import sys
from datetime import datetime as DT
from collections import deque, defaultdict
import glob
import pickle as pl
from pathlib import Path
import numpy as np

from PyQt5 import QtWidgets, uic
from PyQt5.QtWidgets import QAction, QFileDialog
import matplotlib
matplotlib.use('Qt5Agg')
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg, NavigationToolbar2QT as NavigationToolbar
from matplotlib.figure import Figure
import matplotlib.pyplot as plt

#Next we need to create a base class that will load the .ui file in the constructor. It will need to call the __init__ method of the inherited class, load the .ui file into the current object and then show the window.
#https://nitratine.net/blog/post/how-to-import-a-pyqt5-ui-file-in-a-python-gui/
#Open ui 
# $ pyqt5-tools designer

MAIN = "gui/main.ui"
STOCKDIR = "stock"
STOCKCATEGORY = {i:[] for i in os.listdir(STOCKDIR) if os.path.isdir(STOCKDIR+"/"+i)}
STOCKCACHE = defaultdict(lambda : None)
ISSTOCKCACHE = True
TIMEZONE = 'Asia/Taipei'


class _LOG:
    def __init__(self):
        self.INFOHIST = []
        self.WARNHIST = []
        self.LOGHIST = []
        self.ERRHIST = []
        self.MSGHIST = []

        self.INFOINDEX = 0
        self.WARNINDEX = 0
        self.LOGINDEX = 0
        self.ERRINDEX = 0

        self.MSGINDEX = 0

    def info(self, text):
        if len(self.MSGHIST) == 5:
            self.MSGHIST.pop(0)
        self.MSGHIST.append(f"{str(DT.now())}\t[INFO-{self.MSGINDEX}]\t{text}")
        self.MSGINDEX += 1
        self.INFOINDEX += 1
        msg = "\n".join(self.MSGHIST)
        return msg

    def warn(self, text):
        if len(self.MSGHIST) == 5:
            self.MSGHIST.pop(0)
        self.MSGHIST.append(f"{str(DT.now())}\t[WARN-{self.MSGINDEX}]\t{text}")
        self.MSGINDEX += 1
        self.WARNINDEX += 1
        msg = "\n".join(self.MSGHIST)
        return msg

class MplCanvas(FigureCanvasQTAgg):

    def __init__(self, parent, frameWidget, dpi=100):
        assert parent is not None
        w = frameWidget.width()
        h = frameWidget.height()
        self.fig = Figure(figsize=(w, h), dpi=dpi)
        self.axes = self.fig.add_subplot(111)
        self.dataSet = False
        super(MplCanvas, self).__init__()

    # def on_scroll(self, event):
    #     if self.dataSet:
    #         #print(event.button, event.step)
    #         increment = 1 if event.button == 'up' else -1
    #         max_index = self.X.shape[-1] - 1
    #         self.index = np.clip(self.index + increment, 0, max_index)
    #         self.update()

        #fig.canvas.mpl_connect('scroll_event', tracker.on_scroll)
class Ui(QtWidgets.QMainWindow):
    def __init__(self):
        super(Ui, self).__init__() # Call the inherited classes __init__ method
        uic.loadUi(MAIN, self) # Load the .ui file
        self.info('Loading data ...')
        self.path = None
        self.filters = 'Text Files (*.txt)'
        self.quoteDataTuple = (None,None)
        self.frame = MplCanvas(self, self.frameStockDisplay)
        self.frameInit = False

        self.loadPath()
        self.setupMemu()
        self.setupDropDownMenu()
        
        self.info('Application ready.')
        self.show() # Show the GUI

    def setQuoteData(self, quote, fn):
        f = open(fn, 'rb')
        data = pl.loads(f.read())
        if ISSTOCKCACHE:
            STOCKCACHE[quote] = data
        else:
            self.quoteDataTuple = (quote, data)
        f.close()
        return data

    def getQuoteData(self, quote):
        data = None
        if ISSTOCKCACHE:
            data =  STOCKCACHE[quote]
        elif self.quote[0] == quote:
            data = self.quoteDataTuple[1]
        return data

    def loadPath(self):
        global STOCKCATEGORY
        self.path = os.getcwd()
        Log.info(f"Current directory is {self.path}")
        Log.info(f'Categories:{len(STOCKCATEGORY)}')

        for i in STOCKCATEGORY.keys():
            stockDir = os.path.join(STOCKDIR,i)
            pickleFileList = glob.glob(f"{stockDir}/*.pickle")
            pickleFileList = [os.path.basename(j)[:-7] for j in pickleFileList]
            STOCKCATEGORY[i] = pickleFileList

    def setupTriggered(self, category, name):
        def _info():
            self.info(f"Loading {category}:{name}")
            pickleDumpPath = os.path.join(STOCKDIR, category, name + ".pickle")

            if (not os.path.exists(pickleDumpPath)):
                self.warn(f"Fail to load {category}:{name} {pickleDumpPath} not exist")
                return

            self.textQuoteIndex.setText(name)
            data = self.setQuoteData(name, pickleDumpPath)
            self.textEditStartDate.setText(name)

            data.index.tz_convert(TIMEZONE)
            startTime = data.index[0].date()
            endTime = data.index[-1].date()

            self.textEditStartDate.setText(f"{startTime}")
            self.textEditEndDate.setText(f"{endTime}")

            dy = data["Open"].to_numpy()
            dx = data.index.to_pydatetime()
            dx = np.array(list(map(lambda x : x.date(), dx)))

            if not self.frameInit:
                self.frame.axes.plot(dx, dy)
                self.frame.axes.grid()
                #self.frame.axes.set_xticks(rotation=60)
                self.frame.axes.tick_params(axis='x', labelrotation=60)
                toolbar = NavigationToolbar(self.frame, self)
                self.layoutStockDisplay.addWidget(toolbar)
                self.frameInit = True
                self.layoutStockDisplay.addWidget(self.frame)
            else:
                self.frame.axes.cla()
                self.frame.axes.plot(dx, dy)
                self.frame.axes.relim()
                self.frame.axes.autoscale()
                self.frame.axes.grid()
                self.frame.draw()
                # self.ax[i].set_ylim(0, y.max())
                # self.bx[i].set_ydata(self.data[i][1])
                # self.ax[i].set_ylabel(self.data[i][0])
            self.info(f"Max {np.max(dy)}, Min {np.min(dy)}")
        return _info

    def setupDropDownMenu(self):
        self.dropMenuQuotes = QtWidgets.QMenu(self.toolButtonQuotesList)

        for i,stockCatList in STOCKCATEGORY.items():
            menu = QtWidgets.QMenu(f"&{i}", self)
            for quote in stockCatList:
                qact = QtWidgets.QAction(f"&{quote}", self)
                qact.triggered.connect(self.setupTriggered(i, quote))
                menu.addAction(qact)
            self.dropMenuQuotes.addMenu(menu)

        self.toolButtonQuotesList.setPopupMode(QtWidgets.QToolButton.MenuButtonPopup)
        self.toolButtonQuotesList.setMenu(self.dropMenuQuotes)
        action = QtWidgets.QWidgetAction(self.toolButtonQuotesList)
        self.toolButtonQuotesList.menu().addAction(action)
        self.toolButtonQuotesList.clicked.connect(self.printButtonPressed)

    def info(self, msg):
        text = Log.info(msg)
        self.LogBox.setText(text)

    def printButtonPressed(self):
        self.info('Selecting quotes ...')


    def set_title(self, filename=None):
        title = f"{filename if filename else 'Untitled'}"
        self.setWindowTitle(title)

    def confirm_save(self):
        if not self.textEdit0.document().isModified():
            return True

        message = f"Do you want to save changes to {self.path if self.path else 'Untitled'}?"
        MsgBoxBtn = QMessageBox.StandardButton
        MsgBoxBtn = MsgBoxBtn.Save | MsgBoxBtn.Discard | MsgBoxBtn.Cancel

        button = QMessageBox.question(
            self, self.title, message, buttons=MsgBoxBtn
        )

        if button == MsgBoxBtn.Cancel:
            return False

        if button == MsgBoxBtn.Save:
            self.save_document()

        return True

    def new_document(self):
        if self.confirm_save():
            self.textEdit0.clear()
            self.set_title()

    def save_document(self):
        # save the currently openned file
        if (self.path):
            return self.path.write_text(self.textEdit0.toPlainText())

        # save a new file
        filename, _ = QFileDialog.getSaveFileName(
            self, 'Save File', filter=self.filters
        )

        if not filename:
            return

        self.path = Path(filename)
        self.path.write_text(self.textEdit0.toPlainText())
        self.set_title(filename)

    def open_document(self):
        filename, _ = QFileDialog.getOpenFileName(self, filter=self.filters)
        if filename:
            self.path = Path(filename)
            self.textEdit0.setText(self.path.read_text())
            self.set_title(filename)

    def quit(self):
        if self.confirm_save():
            self.destroy()

    def setupMemu(self):
        menuBar = self.menuBar()
        fileMenu = menuBar.addMenu('&File')
        editMenu = menuBar.addMenu('&Edit')
        helpMenu = menuBar.addMenu('&Help')

        actionList = [['&New', 'Create a new document', 'Ctrl+N', self.new_document, 0, 0],
                      ['&Open...', 'Open a document', 'Ctrl+O', self.open_document, 0, 0],
                      ['&Save', 'Save the document', 'Ctrl+S', self.save_document, 0, 0],
                      ['&Exit', 'Exit', 'Alt+F4', self.quit, 1, 0],
                      ['&About', 'About', 'F1', None, 0, 2]]

        menuList = [fileMenu, editMenu, helpMenu]
        for context in actionList:
            key, desc, shortcut, func, isAddSep, menuInd = context
            menu = menuList[menuInd]
            if isAddSep:
                menu.addSeparator()
            act = QAction(key, self)
            act.setStatusTip(desc)
            act.setShortcut(shortcut)
            if func is not None:
                act.triggered.connect(func)
            menu.addAction(act)

        # act = QAction('&New', self)
        # act.setStatusTip('Create a new document')
        # act.setShortcut('Ctrl+N')
        # act.triggered.connect(self.new_document)
        # fileMenu.addAction(act)

        # # open menu item
        # act = QAction('&Open...', self)
        # act.triggered.connect(self.open_document)
        # act.setStatusTip('Open a document')
        # act.setShortcut('Ctrl+O')
        # fileMenu.addAction(act)

        # # save menu item
        # act = QAction('&Save', self)
        # act.setStatusTip('Save the document')
        # act.setShortcut('Ctrl+S')
        # act.triggered.connect(self.save_document)
        # fileMenu.addAction(act)

        # fileMenu.addSeparator()

        # # exit menu item
        # act = QAction('&Exit', self)
        # act.setStatusTip('Exit')
        # act.setShortcut('Alt+F4')
        # act.triggered.connect(self.quit)
        # fileMenu.addAction(act)

        # act = QAction('About', self)
        # act.setStatusTip('About')
        # act.setShortcut('F1')
        # helpMenu.addAction(act)

        # status bar
        self.status_bar = self.statusBar()

Log = _LOG()
app = QtWidgets.QApplication(sys.argv) # Create an instance of QtWidgets.QApplication
window = Ui() # Create an instance of our class
app.exec_() # Start the application

# https://stackoverflow.com/questions/9076332/qt-pyqt-how-do-i-create-a-drop-down-widget-such-as-a-qlabel-qtextbrowser-etc
# from PyQt5 import QtWidgets, QtCore

# class Window(QtWidgets.QMainWindow):
#     def __init__(self):
#         super(Window, self).__init__()
#         layout = QtWidgets.QHBoxLayout(self)
#         self.button = QtWidgets.QToolButton(self)
#         self.button.setPopupMode(QtWidgets.QToolButton.MenuButtonPopup)
#         self.button.setMenu(QtWidgets.QMenu(self.button))
#         self.textBox = QtWidgets.QTextBrowser(self)
#         action = QtWidgets.QWidgetAction(self.button)
#         action.setDefaultWidget(self.textBox)
#         self.button.menu().addAction(action)
#         layout.addWidget(self.button)

# if __name__ == '__main__':

#     import sys
#     app = QtWidgets.QApplication(sys.argv)
#     window = Window()
#     window.resize(100, 60)
#     window.show()
#     sys.exit(app.exec_())

    #     self.button.clicked.connect(self.printButtonPressed) # Remember to pass the definition/method, not the return value!

    #     self.input = self.findChild(QtWidgets.QLineEdit, 'input')

    #     self.show()

    # def printButtonPressed(self):
    #     # This is executed when the button is pressed
    #     print('Input text:' + self.input.text())
