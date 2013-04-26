#!/usr/bin/python


from optparse import OptionParser
import yaml
import psycopg2
import csv
import os
import datetime
import time
import logging
import re

class Singleton(type):
    def __init__(self, *args, **kwargs):
        # Call the superclass (type), because we want Singleton isntances to
        # be intialised *mostly* the same as type isntances
        super(Singleton, self).__init__(*args, **kwargs)
        self.__instance = None

    def __call__(self, *args, **kwargs):
        # If self (the *class* object) has an __instance, return it. Otherwise
        # super-call __call__ to fall back to the normal class-call machinery
        # of calling the class' __new__ then __init__
        if self.__instance is None:
            self.__instance = super(Singleton, self).__call__(*args, **kwargs)
        return self.__instance


class ArgumentParser:
	"""Commandline arguments"""

	__metaclass__ = Singleton
	options = ""
	args = ""

	def __init__(self):
		self.parse()

	def parse(self):
		parser = OptionParser()

		parser.add_option("-v", "--verbose",
                  action="store_true", dest="verbose", default=False,
                  help="Print out messages")

		parser.add_option("-i", "--input", dest="input", default="query.yml",
                  help="Input queries in yml. Default is ./query.yml")

		parser.add_option("-d", "--data", dest="data", default=False,
                  action="store_true", help="Write data to csv file")

		parser.add_option("-o", "--output", dest="output", default="output", help="Output folder. Default is ./output")

		(self.options, self.args) = parser.parse_args()

	def isValid(self):
		return True

	def getInput(self):
		return self.options.input

	def isVerbose(self):
		return self.options.verbose

	def getOutput(self):
		return self.options.output
	
	def isOutputToCsv(self):
		return self.options.data

	def printUsage(self):
		self.parser.print_help()

	def printMe(self):
		print(self.options)
		print(self.args)

class LoggerFactory:

	arguments = None

	@staticmethod
	def createLogger(name):
		logging.basicConfig(format='%(asctime)s %(name)s:%(lineno)s %(message)s', level=LoggerFactory._createLevel_(LoggerFactory._isVerbose_()))
		return logging.getLogger(name)
		
	@staticmethod
	def _createLevel_(verbose):
		level = logging.WARNING
		if verbose:
			level = logging.DEBUG

		return level

	@staticmethod
	def _isVerbose_():
		arguments = ArgumentParser()
		return arguments.isVerbose()

class DatabaseAdapter:
	"""Database adapter"""

	conn = ""
	cursor = ""
	resultset = ""
	columns = ""
	logger = None

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

	def getIdentifier(self):
		return self._formatFilename_(self.group + ' ' + self.name)

	def _formatFilename_(self, value):
		import unicodedata
		value = unicodedata.normalize('NFKD', unicode(value)).encode('ascii', 'ignore')
		value = unicode(re.sub('[^\w\s-]', '', value).strip().lower())
		value = unicode(re.sub('[-\s]+', '-', value)) 
		return value
		
class BaseWriter:
	"""Base Writer"""
	logger = None
	arguments = None
	
	def createDirIfNotExist(self):
		if not os.path.exists(self._getOutputFolder_()):
			os.makedirs(self._getOutputFolder_())


	def _getArguments_(self):
		if self.arguments == None:
			self.arguments = ArgumentParser()
		return self.arguments

	def _getOutputFolder_(self):
		return self._getArguments_().getOutput()


	def _getLogger_(self):
		if self.logger == None:
			self.logger = LoggerFactory.createLogger(self.__class__.__name__)
		return self.logger
	

class OutputWriter(BaseWriter):
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
	
	def debug(self):
		self._getLogger_().debug(self.name)
		self._getLogger_().debug(self.columns)
		self._getLogger_().debug(self.resultset)
		
class SummaryWriter(BaseWriter):
	"""Summary Writer"""

	logger = None
	arguments = None
	
	def __init__(self, commands):
		self.commands = commands
		
	def write(self):
		self.createDirIfNotExist()
		with open(self._getOutputFolder_()+'/'+'summary.csv','wb') as f:
			writer = csv.writer(f)
			data = [('group','name','query','time taken')]
			writer.writerows(data)
			for commandIdentifier in self.commands:
				writer.writerows(self.commands[commandIdentifier].getIterableData())
				
			f.close()

class InputParser:
	"""Parsing input file"""

	arguments = None
	yamlInput = None
	commands = {}
	yamlInput = None

	def loadYaml(self):
		f = file(self._getArguments_().getInput())
		self.yamlInput = yaml.load(f)
		f.close

	def printYaml(self):
		self._getLogger_().debug(yaml.dump(self.yamlInput))
		self._getLogger_().debug(self.yamlInput)
		
	def parseConfigToCommands(self):
		for groupName in self.yamlInput.keys():
			queryItem = self.yamlInput[groupName]
			for queryKey in queryItem:
				queryValues = queryItem[queryKey]
				queryObject = Command(groupName, queryValues['name'], queryValues['query'])
				self.commands.update({queryObject.getIdentifier():queryObject})
				
	def getCommands(self):
		return self.commands

	def _isOutputToCsv_(self):
		return self._getArguments_().isOutputToCsv()
		
	def executeCommands(self):
		db = DatabaseAdapter()
		for commandIdentifier in self.commands.keys():
			db.connect()
			start = time.time()
			db.execute(self.commands[commandIdentifier].query)
			end = time.time()
			timeTaken = end - start
			self.commands[commandIdentifier].setTimeTaken(timeTaken)
			db.close()
			if self._isOutputToCsv_():
				self._writeDataToFile_(commandIdentifier, db.getColumns(), db.getData(), self._getOutputFolder_())
	
	def _writeDataToFile_(self, commandIdentifier, columns, data, outputFolder):			
			writer = OutputWriter(commandIdentifier, columns, data, outputFolder)
			if(self.arguments.isVerbose()):
				writer.debug()
			writer.write()			

	def _getArguments_(self):
		if self.arguments == None:
			self.arguments = ArgumentParser()
		return self.arguments

	def _getOutputFolder_(self):
		return self._getArguments_().getOutput()


	def _getLogger_(self):
		if self.logger == None:
			self.logger = LoggerFactory.createLogger(self.__class__.__name__)
		return self.logger


def main():
	arguments = ArgumentParser()
	
	if arguments.isValid() :
		
		if(arguments.isVerbose()):
			arguments.printMe()

		inputParser = InputParser()
		inputParser.loadYaml()
		
		
		inputParser.parseConfigToCommands()
		inputParser.executeCommands()
				
		summaryWriter = SummaryWriter(inputParser.getCommands())
		summaryWriter.write()
	else:
		arguments.printUsage()

if __name__ == "__main__":
	main()


	
