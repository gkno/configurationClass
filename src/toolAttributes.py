#!/bin/bash/python

from __future__ import print_function
import networkx as nx
from copy import deepcopy

import errors
from errors import *

import json
import os
import sys

class toolAttributes:
  def __init__(self):
    self.arguments   = {}
    self.description = ''
    self.executable  = ''
    self.isHidden    = ''
    self.modifier    = ''
    self.path        = ''
    self.precommand  = ''

class toolArguments:
  def __init__(self):
    self.allowedExtensions        = []
    self.allowMultipleDefinitions = False
    self.description              = ''
    self.hasType                  = None
    self.isInput                  = False
    self.isOutput                 = False
    self.isRequired               = False
    self.shortForm                = ''

class toolConfiguration:
  def __init__(self):
    self.attributes           = {}
    self.availableTools       = {}
    self.errors               = errors()
    self.filename             = ''
    self.jsonError            = ''
    self.setRequiredArguments = False

  # Open a configuration file and store the contents of the file in the
  # configuration dictionary.
  def readConfigurationFile(self, filename):
    fileExists = False
    jsonError  = True
    errorText  = ''

    try: jsonData = open(filename)
    except: return fileExists, jsonError, errorText
    fileExists    = True
    self.filename = filename

    try: configurationData = json.load(jsonData)
    except:
      exc_type, exc_value, exc_traceback = sys.exc_info()
      errorText = exc_value
      return fileExists, jsonError, errorText

    jsonError = False

    return fileExists, jsonError, errorText, configurationData

  #TODO
  # Validate the contents of the tool configuration file.
  def processConfigurationFile(self, data, toolFile):

    # First validate the contents of the data structure.
    success = self.validateConfigurationData(data)
    if not success: return False

    # Now put all of the data into data structures.
    for toolName in data['tools']:
      if toolName in self.availableTools:
        print('Non-unique tool name error:', toolName)
        return False

      self.availableTools[toolName] = toolFile
      self.attributes[toolName]     = toolAttributes()

      # Set the general tool attributes.
      self.attributes[toolName]             = toolAttributes()
      self.attributes[toolName].description = data['tools'][toolName]['description']
      self.attributes[toolName].executable  = data['tools'][toolName]['executable']
      if 'hide tool' in data['tools'][toolName]: self.attributes[toolName].isHidden  = data['tools'][toolName]['hide tool']
      self.attributes[toolName].modifier    = data['tools'][toolName]['modifier'] if 'modifier' in data['tools'][toolName] else ''
      self.attributes[toolName].path        = data['tools'][toolName]['path']
      self.attributes[toolName].precommand  = data['tools'][toolName]['precommand'] if 'precommand' in data['tools'][toolName] else ''
  
      # Set the tool argument information.
      for argument in data['tools'][toolName]['arguments']:
        if argument not in self.attributes[toolName].arguments: self.attributes[toolName].arguments[argument] = toolArguments()
        contents = data['tools'][toolName]['arguments'][argument]
  
        # If multiple extensions are allowed, they will be separated by pipes in the configuration
        # file.  Add all allowed extensions to the list.
        extension = contents['extension']
        if '|' in extension:
          extensions = extension.split('|')
          for extension in extensions: self.attributes[toolName].arguments[argument].allowedExtensions.append(extension)
        else: self.attributes[toolName].arguments[argument].allowedExtensions.append(extension)
  
        if 'allow multiple definitions' in contents: self.attributes[toolName].arguments[argument].allowMultipleDefinitions = contents['allow multiple definitions']
        self.attributes[toolName].arguments[argument].description              = contents['description']
        self.attributes[toolName].arguments[argument].hasType                  = contents['type']
        self.attributes[toolName].arguments[argument].isInput                  = contents['input']
        self.attributes[toolName].arguments[argument].isOutput                 = contents['output']
        self.attributes[toolName].arguments[argument].isRequired               = contents['required']
        if 'short form' in contents: self.attributes[toolName].arguments[argument].shortForm = contents['short form']

  # Validate the contents of the tool configuration file.
  def validateConfigurationData(self, data):
    return True

  # Check if a tool argument corresponds to a file.
  def isArgumentAFile(self, tool, argument):
    isDummy  = False
    isInput  = False
    isOutput = False

    try:
      isInput = self.attributes[tool].arguments[argument].isInput
      isOutput = self.attributes[tool].arguments[argument].isOutput
    except:
      isDummy = self.isArgumentAFileError(tool, argument)

    if isInput or isOutput or isDummy: return True
    else: return False

  # If there was an error trying to determine if a tool argument corresponded to a file, 
  # determine the source of the error and then call the errors class to fail.
  def isArgumentAFileError(self, tool, argument):

    # Check if the tool is valid.
    if tool not in self.attributes: self.errors.invalidTool(tool, 'toolConfiguration -> isArgumentAFile')

    # Check if there is an arguments block associated with this tool.
    try: arguments = self.attributes[tool].arguments
    except: self.errors.noArgumentsInformation(tool, 'toolConfiguration -> isArgumentAFile')

    # Check that there is information about the specified argument.  If the argument is
    # 'dummy-file', the node corresponds to a file that does not have a command line argument.
    # In this case, return to the previous function without failing.
    if argument == 'dummy': return True
    if argument not in self.attributes[tool].arguments: self.errors.invalidArgument(tool, argument, 'toolConfiguration -> isArgumentAFile')
