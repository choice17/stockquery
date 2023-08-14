from datetime import datetime as DT
import traceback

class LOG:

	def __init__(self):
		self.logger = None
		self.logLevel = 0
		self.error = 0
		self.warn = 0

	def info(self, *args):
		size = len(args)
		print(f"{DT.now()} [INFO] {'%s' * size % args}")

	def warn(self, *args):
		self.warn += 1
		print(f"{DT.now()} [WARN] {'%s' * size % args}")

	def error(self, *args):
		self.error += 1
		print(f"{DT.now()} [ERROR] {'%s' * size % args}")

Log = LOG()
