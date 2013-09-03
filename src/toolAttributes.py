#!/bin/bash/python

from __future__ import print_function
import networkx as nx
from copy import deepcopy

import configurationClassErrors
from configurationClassErrors import *

import nodeAttributes
from nodeAttributes import *

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
    self.errors               = configurationClassErrors()
    self.filename             = ''
    self.jsonError            = ''
    self.nodeMethods          = nodeClass()
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

      # Set the general tool attributes.
      self.attributes[toolName]             = toolAttributes()
      self.setToolAttribute(self.attributes, toolName, 'description', data['tools'][toolName]['description'])
      self.setToolAttribute(self.attributes, toolName, 'executable', data['tools'][toolName]['executable'])
      if 'modifier' in data['tools'][toolName]: self.setToolAttribute(self.attributes, toolName, 'modifier', data['tools'][toolName]['modifier'])
      self.setToolAttribute(self.attributes, toolName, 'path', data['tools'][toolName]['path'])
      if 'precommand' in data['tools'][toolName]: self.setToolAttribute(self.attributes, toolName, 'precommand', data['tools'][toolName]['precommand'])
      if 'hide tool' in data['tools'][toolName]: self.setToolAttribute(self.attributes, toolName, 'isHidden', data['tools'][toolName]['hide tool'])
  
      # Set the tool argument information.
      for argument in data['tools'][toolName]['arguments']:

        # The information about the arguments is stored in a node data structure.  This allow all of the methods
        # for nodes to be used with each of the tool arguments.  These nodes are not added to the graph.  Begin
        # by determining if the argument is for a file or an option and set the nodeAttributes accordingly.
        if argument not in self.attributes[toolName].arguments:
          contents = data['tools'][toolName]['arguments'][argument]

          # Deal with file nodes.
          if contents['input'] or contents['output']:
            attributes             = fileNodeAttributes()
            self.nodeMethods.setNodeAttribute(attributes, 'description', contents['description'])
            self.nodeMethods.setNodeAttribute(attributes, 'isInput', contents['input'])
            self.nodeMethods.setNodeAttribute(attributes, 'isOutput', contents['output'])
            self.nodeMethods.setNodeAttribute(attributes, 'isRequired', contents['required'])
            if 'short form argument' in contents: self.nodeMethods.setNodeAttribute(attributes, 'shortForm', contents['short form argument'])

            # If multiple extensions are allowed, they will be separated by pipes in the configuration
            # file.  Add all allowed extensions to the list.
            extension = contents['extension']
            if '|' in extension:
              extensions = extension.split('|')
              self.nodeMethods.setNodeAttribute(attributes, 'allowedExtensions', extensions)

            #else: attributes.allowedExtensions.append(extension)
            else:
              extensions = []
              extensions.append(extension)
              self.nodeMethods.setNodeAttribute(attributes, 'allowedExtensions', extensions)

          # Otherwise the argument requires an option node.
          else:
            attributes             = optionNodeAttributes()
            self.nodeMethods.setNodeAttribute(attributes, 'description', contents['description'])
            self.nodeMethods.setNodeAttribute(attributes, 'dataType', contents['type'])
            self.nodeMethods.setNodeAttribute(attributes, 'isRequired', contents['required'])
            if 'short form argument' in contents: self.nodeMethods.setNodeAttribute(attributes, 'shortForm', contents['short form argument'])
            if 'allow multiple values' in contents: self.nodeMethods.setNodeAttribute(attributes, 'allowMultipleValues', contents['allow multiple values'])

        self.attributes[toolName].arguments[argument] = attributes

  # Validate the contents of the tool configuration file.
  def validateConfigurationData(self, data):
    return True

  # Set a value in the toolAttributes.
  def setToolAttribute(self, attributes, tool, attribute, value):
    try: test = getattr(attributes[tool], attribute)

    # If the attribute can't be set, determine the source of the problem and provide an
    # error message.
    except:

      # If the tool is not available.
      if tool not in attributes: self.errors.invalidToolInSetToolAttribute(tool)

      # If the attribute being set is not valid.
      if attribute not in attributes: self.errors.invalidAttributeInSetToolAttribute(attribute)

    # Set the attribute.
    setattr(attributes[tool], attribute, value)

  # Get the long form of a tool argument.
  def getLongFormArgument(self, tool, argument):

    # Check if the supplied argument is in the arguments data structure already.  If so, it is already
    # in the long form.
    if argument in self.attributes[tool].arguments: return argument

    # If the argument was not in the data structure, loop over the arguments and check their
    # short form versions.  If the supplied argument appears as a short form, return the long
    # form associated with it.
    for toolArgument in self.attributes[tool].arguments:
      try: shortForm = self.attributes[tool].arguments[toolArgument].shortForm
      except: shortForm = ''

      if shortForm == argument: return toolArgument

    # If no value has been returned, the supplied argument is not associated with this tool.
    self.errors.invalidToolArgument(tool, argument, 'getLongFormArgument')
