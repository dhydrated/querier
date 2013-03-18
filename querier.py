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

class Command:
	"""Command values"""
	name = ""
	query = ""
	
	def __init__(self,name,query):
		self.name = name
		self.query = query
		
	def __str__(self):
		return self.name
		
		
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
		
	def iterateCommands(self):
		for commandName in self.commands.keys():
			print self.commands[commandName]

def main():
	parser = Parser()
	
	if(parser.verbose()):
		parser.printMe()

	input = InputParser(parser)
	input.loadYaml()
	input.printYaml()
	input.parseConfig()
	input.iterateCommands()

if __name__ == "__main__":
	main()


	