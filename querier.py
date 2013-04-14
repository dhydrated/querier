#!/usr/bin/python


from optparse import OptionParser
import yaml
import psycopg2
import csv
import os
import datetime
import time

class ArgumentParser:
	"""Commandline arguments"""

	options = ""
	args = ""

	def parse(self):
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

	def isValid(self):
		return True

	def getInput(self):
		return self.options.input

	def isVerbose(self):
		return self.options.verbose

	def getOutput(self):
		return self.options.output

	def printUsage(self):
		self.parser.print_help()

	def printMe(self):
		print(self.options)
		print(self.args)

class Logger:
	"""Script logger"""

	def __init__(self, arguments):
		self.arguments = arguments
		self.verbose = self.arguments.isVerbose()

	def debug(self, msg):
		if self.verbose :
			self._print_(msg)

	def info(self, msg):
		self._print_(msg)
			
	def _print_(self,msg):
		print str(datetime.datetime.today())+" : "+str(msg)

class DatabaseAdapter:
	"""Database adapter"""

	conn = ""
	cursor = ""
	resultset = ""
	columns = ""
	
	def __init__(self, logger):
		self.logger = logger

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
	
	group = None
	name = None
	query = None
	timeTaken = None
	
	def __init__(self,group,name,query):
		self.name = name
		self.query = query
		self.group = group
		
	def __str__(self):
		return self.group + ", " + self.name + ", " + self.query + ", " + str(self.timeTaken)
	
	def setTimeTaken(self, timeTaken):
		self.timeTaken = timeTaken
		
	def getIterableData(self):
		return [(self.group, self.name, self.query, self.timeTaken)]
		
class BaseWriter:
	"""Base Writer"""
	
	def createDirIfNotExist(self):
		if not os.path.exists(self.output):
			os.makedirs(self.output)
	

class OutputWriter(BaseWriter):
	"""Output writer"""

	def __init__(self, logger, name, columns, resultset, output):
		self.name = name
		self.columns = columns
		self.resultset = resultset
		self.output = output
		self.logger = logger

	def write(self):
		
		self.createDirIfNotExist()

		with open(self.output+'/'+self.name+'.csv', 'wb') as f:
			writer = csv.writer(f)
			data = self.columns + self.resultset
			writer.writerows(data)
	
	def debug(self):
		self.logger.debug(self.name)
		self.logger.debug(self.columns)
		self.logger.debug(self.resultset)
		
class SummaryWriter(BaseWriter):
	"""Summary Writer"""
	
	def __init__(self, logger, commands, output):
		self.logger = logger
		self.commands = commands
		self.output = output
		
	def write(self):
		with open(self.output+'/'+'summary.csv','wb') as f:
			writer = csv.writer(f)
			data = [('group','name','query','time taken')]
			writer.writerows(data)
			for commandName in self.commands:
				writer.writerows(self.commands[commandName].getIterableData())

class InputParser:
	"""Parsing input file"""

	arguments = ""
	yamlInput = ""
	commands = {}

	def __init__(self, logger, arguments):
		self.arguments = arguments
		self.logger = logger

	def loadYaml(self):
		f = file(self.arguments.getInput())
		self.yamlInput = yaml.load(f)
		f.close

	def printYaml(self):
		self.logger.debug(yaml.dump(self.yamlInput))
		self.logger.debug(self.yamlInput)
		
	def parseConfig(self):
		for rootKey in self.yamlInput.keys():
			queryItem = self.yamlInput[rootKey]
			for queryKey in queryItem:
				queryValues = queryItem[queryKey]
				queryObject = Command(rootKey, queryValues['name'], queryValues['query'])
				self.commands.update({queryObject.name:queryObject})
				
	def getCommands(self):
		return self.commands
		
	def executeCommands(self):
		db = DatabaseAdapter(self.logger)
		for commandName in self.commands.keys():
			db.connect()
			start = time.time()
			db.execute(self.commands[commandName].query)
			end = time.time()
			timeTaken = end - start
			self.commands[commandName].setTimeTaken(timeTaken)
			db.close()
			self._writeDataToFile_(commandName, db.getColumns(), db.getData(), self.arguments.getOutput())
	
	def _writeDataToFile_(self, commandName, columns, data, outputFolder):			
			writer = OutputWriter(self.logger, commandName, columns, data, outputFolder)
			
			if(self.arguments.isVerbose()):
				writer.debug()
			
			writer.write()

def main():
	arguments = ArgumentParser()
	arguments.parse()
	
	if arguments.isValid() :
		logger = Logger(arguments)
		
		if(arguments.isVerbose()):
			arguments.printMe()

		inputParser = InputParser(logger, arguments)
		inputParser.loadYaml()
		
		if(arguments.isVerbose()):
			inputParser.printYaml()
		
		inputParser.parseConfig()
		inputParser.executeCommands()
		
		#for commandName in inputParser.getCommands(): 
		#	logger.debug(inputParser.getCommands()[commandName])
		
		summaryWriter = SummaryWriter(logger, inputParser.getCommands(), arguments.getOutput())
		summaryWriter.write()
	else:
		arguments.printUsage()

if __name__ == "__main__":
	main()


	
