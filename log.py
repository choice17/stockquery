from datetime import datetime as DT
class LOG:

	def __init__(self):
		self.logger = None

	def info(self, *args):
		print(f"{DT.now()} [INFO] {args}")

	def warn(self, *args):
		print(f"{DT.now()} [WARN] {args}")

Log = LOG()
