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

		parser.add_option("-c", "--config", dest="config", default="config.yml",
                  help="Configuration in yml. Default is ./config.yml")

		parser.add_option("-d", "--delimiter", dest="delimiter", default="|",
                  help="Delimiter for output csv. Default is |")

		parser.add_option("-w", "--write", dest="write", default=False,
                  action="store_true", help="Write data to csv file")

		parser.add_option("-o", "--output", dest="output", default="output", help="Output folder. Default is ./output")

		(self.options, self.args) = parser.parse_args()

	def isValid(self):
		return True

	def getInput(self):
		return self.options.input

	def getConfig(self):
		return self.options.config

	def isVerbose(self):
		return self.options.verbose

	def getOutput(self):
		return self.options.output

	def getDelimiter(self):
		return self.options.delimiter
	
	def isOutputToCsv(self):
		return self.options.write

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
	database = None

	def __init__(self, database):
		self.database = database
		
	def connect(self):
		self.conn = psycopg2.connect("host=%s port=%s dbname=%s user=%s password=%s" % (self.database.host, self.database.port, self.database.dbname, self.database.username, self.database.password))

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


class Group:

	queries = []
	name = None

	def __init__(self, name):
		self.queries = []
		self.name = name

	def addQuery(self, query):
		self.queries.append(query)

	def getQueries(self):
		return self.queries


class Query:
	"""Query values"""
	
	name = None
	query = None
	timeTaken = None
	
	def __init__(self,group,name,query):
		self.name = name
		self.query = query
		self.group = group
		
	def __str__(self):
		return self.group.name + ", " + self.name + ", " + self.query + ", " + str(self.timeTaken)
	
	def setTimeTaken(self, timeTaken):
		self.timeTaken = timeTaken
		
	def getIterableData(self):
		return [(self.group.name, self.name, self.query, self.timeTaken)]

	def getIdentifier(self):
		return self._formatFilename_(self.group.name + ' ' + self.name)

	def _formatFilename_(self, value):
		import unicodedata
		value = unicodedata.normalize('NFKD', unicode(value)).encode('ascii', 'ignore')
		value = unicode(re.sub('[^\w\s-]', '', value).strip().lower())
		value = unicode(re.sub('[-\s]+', '-', value)) 
		return value


class Database:

	host = None
	port = None
	dbname = None
	username = None
	password = None

	def __init__(self, host, port, dbname, username, password):
		self.host = host
		self.port = port
		self.dbname = dbname
		self.username = username
		self.password = password

		
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
			writer = csv.writer(f, delimiter='|')
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
	groups = None
	
	def __init__(self, groups):
		self.groups = groups
		
	def write(self):
		self.createDirIfNotExist()
		with open(self._getOutputFolder_()+'/'+'summary.csv','wb') as f:
			writer = csv.writer(f, delimiter='|')
			data = [('group','name','query','time taken')]
			writer.writerows(data)
			for group in self.groups:
				for query in group.getQueries():
					writer.writerows(query.getIterableData())
				
			f.close()

class YamlParser:

	yamlFile = None
	arguments = None
	logger = None

	def _loadYaml_(self):
		f = file(self._getFilePath_())
		self.yamlFile = yaml.load(f)
		f.close

	def printYaml(self):
		self._getLogger_().debug(yaml.dump(self.yamlFile))
		self._getLogger_().debug(self.yamlFile)		

	def _getArguments_(self):
		if self.arguments == None:
			self.arguments = ArgumentParser()
		return self.arguments

	def _getLogger_(self):
		if self.logger == None:
			self.logger = LoggerFactory.createLogger(self.__class__.__name__)
		return self.logger

	def _getFilePath_(self):
		pass


class ConfigParser(YamlParser):

	def __init__(self):
		self._loadYaml_()
		self._parse_()

	def _parse_(self):
		database = self.yamlFile['database']
		self.database = Database(host = database['host'], port = database['port'], dbname = database['dbname'], username = database['username'], password = database['password'])
			
	def _getFilePath_(self):
		return self._getArguments_().getConfig()

	def getDatabase(self):
		return self.database


class QueryParser(YamlParser):
	"""Parsing input file"""

	groups = []
	database = None

	def __init__(self, database):
		self.database = database
		self._loadYaml_()
		self._parse_()
		
	def _parse_(self):
		for groupName in reversed(self.yamlFile.keys()):
			group = Group(groupName)
			queries = self.yamlFile[groupName]
			for queryInfo in queries:
				queryObject = Query(group, queryInfo['name'], queryInfo['query'])
				group.addQuery(queryObject)
			self.groups.append(group)
			group = None
				
	def getGroups(self):
		return self.groups

	def _isOutputToCsv_(self):
		return self._getArguments_().isOutputToCsv()
		
	def executeCommands(self):
		db = DatabaseAdapter(self.database)

		for group in self.groups:
			for query in group.queries:
				db.connect()
				start = time.time()
				db.execute(query.query)
				end = time.time()
				timeTaken = end - start
				query.setTimeTaken(timeTaken)
				db.close()
				if self._isOutputToCsv_():
					self._writeDataToFile_(query.getIdentifier(), db.getColumns(), db.getData(), self._getOutputFolder_())
	
	def _writeDataToFile_(self, commandIdentifier, columns, data, outputFolder):			
			writer = OutputWriter(commandIdentifier, columns, data, outputFolder)
			if(self.arguments.isVerbose()):
				writer.debug()
			writer.write()	

	def _getOutputFolder_(self):
		return self._getArguments_().getOutput()

	def _getFilePath_(self):
		return self._getArguments_().getInput()


def main():
	arguments = ArgumentParser()
	
	if arguments.isValid() :
		
		if(arguments.isVerbose()):
			arguments.printMe()

		configParser = ConfigParser()

		queryParser = QueryParser(configParser.getDatabase())
		queryParser.executeCommands()
				
		summaryWriter = SummaryWriter(queryParser.getGroups())
		summaryWriter.write()
	else:
		arguments.printUsage()

if __name__ == "__main__":
	main()


	
