#!/usr/bin/python


from optparse import OptionParser
import yaml

class Parser:
	"""Commandline parser"""

	options = ""
	args = ""

	def __init__(self):
		parser = OptionParser()
		parser.add_option("-f", "--file", dest="filename",
                  help="write report to FILE", metavar="FILE")
		parser.add_option("-v", "--verbose",
                  action="store_true", dest="verbose", default=False,
                  help="print out messages")
		parser.add_option("-i", "--input", dest="input",
                  help="input")

		(self.options, self.args) = parser.parse_args()

	def input(self):
		return self.options.input

	def verbose(self):
		return self.options.verbose

	def printMe(self):
		print(self.options)
		print(self.args)

class QueryExecutor:
	"""Query Executor based on the input file"""

	parser = ""

	def __init__(self, parser):
		self.parser = parser

def main():
	parser = Parser()
	
	if(parser.verbose()):
		parser.printMe()

	queryExecutor = QueryExecutor(parser)

if __name__ == "__main__":
	main()


