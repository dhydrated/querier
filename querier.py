#!/usr/bin/python


from optparse import OptionParser
import yaml
import psycopg2
import csv

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
		parser.add_option("-i", "--input", dest="input", default="query.yml",
                  help="input")

		(self.options, self.args) = parser.parse_args()

	def input(self):
		return self.options.input

	def verbose(self):
		return self.options.verbose

	def printMe(self):
		print(self.options)
		print(self.args)

class DatabaseAdapter:
	"""Database adapter"""

	conn = ""
	cursor = ""
	resultset = ""
	columns = ""

	def connect(self):
		self.conn = psycopg2.connect("dbname=testdb user=taufekj password=password")

	def execute(self, query):
		self.cursor = self.conn.cursor()
		self.cursor.execute(query)
		self.columns = [desc[0] for desc in self.cursor.description]
		self.resultset = self.cursor.fetchall()

	def close(self):
		self.cursor.close()
		self.conn.close()

	def getData(self):
		return self.resultset

	def getColumns(self):
		return self.columns

class Command:
	"""Command values"""
	name = ""
	query = ""
	
	def __init__(self,name,query):
		self.name = name
		self.query = query
		
	def __str__(self):
		return self.name
		

class OutputWriter:
	"""Output writer"""

	def __init__(self, name, columns, resultset):
		self.name = name
		self.columns = columns
		self.resultset = resultset

	def write(self):
		pass	

	def debug(self):
		print self.name
		print self.columns
		print self.resultset

class InputParser:
	"""Parsing input file"""

	parser = ""
	yamlInput = ""
	commands = {}

	def __init__(self, parser):
		self.parser = parser

	def loadYaml(self):
		f = file(self.parser.input())
		self.yamlInput = yaml.load(f)
		f.close

	def printYaml(self):
		print yaml.dump(self.yamlInput)
		print self.yamlInput
		
	def parseConfig(self):
		for rootKey in self.yamlInput.keys():
			queryItem = self.yamlInput[rootKey]
			#print queryItem
			for queryKey in queryItem:
				queryValues = queryItem[queryKey]
				#print queryItem[queryKey]
				#for queryObjectKey in queryValues:
				#	print queryValues[queryObjectKey]
				#print queryValues['name'];
				#print queryValues['query'];
				queryObject = Command(queryValues['name'], queryValues['query'])
				#print queryObject.name
				self.commands.update({queryObject.name:queryObject})
				
	def getCommands(self):
		return self.commands
		
	def executeCommands(self):

		db = DatabaseAdapter()
		for commandName in self.commands.keys():
			db.connect()
			db.execute(self.commands[commandName].query)
			writer = OutputWriter(commandName, db.getData(), db.getColumns())
			db.close()

			writer.debug()

def main():
	parser = Parser()
	
	if(parser.verbose()):
		parser.printMe()

	input = InputParser(parser)
	input.loadYaml()
	input.printYaml()
	input.parseConfig()
	input.executeCommands()

if __name__ == "__main__":
	main()


	
