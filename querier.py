#!/usr/bin/python


from optparse import OptionParser
import yaml
import psycopg2
import csv
import os

class ArgumentParser:
	"""Commandline arguments"""

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

		parser.add_option("-o", "--output", dest="output", default="output", help="output")

		(self.options, self.args) = parser.parse_args()

	def input(self):
		return self.options.input

	def verbose(self):
		return self.options.verbose

	def output(self):
		return self.options.output

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
		self._execute_(query)
		self._setAttributes_()

	def _execute_(self, query):
		self.cursor = self.conn.cursor()
		self.cursor.execute(query)

	def _setAttributes_(self):
		self.columns = [desc[0] for desc in self.cursor.description]
		self.columns = [tuple(self.columns)]
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

	def __init__(self, name, columns, resultset, output):
		self.name = name
		self.columns = columns
		self.resultset = resultset
		self.output = output

	def write(self):
		
		self.createDirIfNotExist()

		with open(self.output+'/'+self.name+'.csv', 'wb') as f:
			writer = csv.writer(f)
			data = self.columns + self.resultset
			writer.writerows(data)
	
	def createDirIfNotExist(self):
		if not os.path.exists(self.output):
			os.makedirs(self.output)
	
	def debug(self):
		print self.name
		print self.columns
		print self.resultset

class InputParser:
	"""Parsing input file"""

	arguments = ""
	yamlInput = ""
	commands = {}

	def __init__(self, arguments):
		self.arguments = arguments

	def loadYaml(self):
		f = file(self.arguments.input())
		self.yamlInput = yaml.load(f)
		f.close

	def printYaml(self):
		print yaml.dump(self.yamlInput)
		print self.yamlInput
		
	def parseConfig(self):
		for rootKey in self.yamlInput.keys():
			queryItem = self.yamlInput[rootKey]
			for queryKey in queryItem:
				queryValues = queryItem[queryKey]
				queryObject = Command(queryValues['name'], queryValues['query'])
				self.commands.update({queryObject.name:queryObject})
				
	def getCommands(self):
		return self.commands
		
	def executeCommands(self):
		db = DatabaseAdapter()
		for commandName in self.commands.keys():
			db.connect()
			db.execute(self.commands[commandName].query)
			db.close()
			self._writeDataToFile_(commandName, db.getColumns(), db.getData(), self.arguments.output())
	
	def _writeDataToFile_(self, commandName, columns, data, outputFolder):			
			writer = OutputWriter(commandName, columns, data, outputFolder)
			
			if(self.arguments.verbose()):
				writer.debug()
			
			writer.write()

def main():
	arguments = ArgumentParser()
	
	if(arguments.verbose()):
		arguments.printMe()

	input = InputParser(arguments)
	input.loadYaml()
	
	if(arguments.verbose()):
		input.printYaml()
	
	input.parseConfig()
	input.executeCommands()

if __name__ == "__main__":
	main()


	
