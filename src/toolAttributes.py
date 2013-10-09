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

class toolConfiguration:
  def __init__(self):
    self.attributes           = {}
    self.availableTools       = {}
    self.configurationData    = {}
    self.errors               = configurationClassErrors()
    self.filename             = ''
    self.jsonError            = ''
    self.nodeMethods          = nodeClass()
    self.setRequiredArguments = False

  # Process the tool data.
  def processConfigurationData(self, tool, data):

    # Validate the information.
    success = self.validateConfigurationData(tool, data)

    # Include the tool in the list of available tools.
    self.availableTools[tool] = tool

    # Set the general tool attributes.
    attributes = toolAttributes()
    self.setToolAttribute(attributes, tool, 'description', self.configurationData[tool]['description'])
    self.setToolAttribute(attributes, tool, 'executable', self.configurationData[tool]['executable'])
    if 'modifier' in self.configurationData[tool]: self.setToolAttribute(attributes, tool, 'modifier', self.configurationData[tool]['modifier'])
    self.setToolAttribute(attributes, tool, 'path', self.configurationData[tool]['path'])
    if 'precommand' in self.configurationData[tool]: self.setToolAttribute(attributes, tool, 'precommand', self.configurationData[tool]['precommand'])
    if 'hide tool' in self.configurationData[tool]: self.setToolAttribute(attributes, tool, 'isHidden', self.configurationData[tool]['hide tool'])
    self.attributes[tool] = attributes

  # Validate the contents of the tool configuration file.
  def validateConfigurationData(self, tool, data):
    self.configurationData[tool] = data
    return True

  # Get information about a tool argument from the configuration data.
  def getArgumentData(self, tool, argument, attribute):
    try: value = self.configurationData[tool]['arguments'][argument][attribute]
    except:

      #FIXME Sort all the errors.
      if tool not in self.configurationData:
        print('MISSING TOOL: tools.getArgumentData', tool)
        self.errors.terminate()

      if argument not in self.configurationData[tool]['arguments']:
        print('MISSING ARGUMENT: tools.getArgumentData', tool, argument, attribute)
        print(self.configurationData[tool])
        self.errors.terminate()

      if attribute not in self.configurationData[tool]['arguments'][argument]:
        print('MISSING ATTRIBUTE: tools.getArgumentData', tool, argument)
        return ''

    return value

  def buildNodeFromToolConfiguration(self, tool, argument):
  
    # Set the tool argument information.
    contents = self.configurationData[tool]['arguments'][argument]
    attributes = optionNodeAttributes()
    self.nodeMethods.setNodeAttribute(attributes, 'dataType', contents['type'])
    self.nodeMethods.setNodeAttribute(attributes, 'description', contents['description'])
    self.nodeMethods.setNodeAttribute(attributes, 'isInput', contents['input'])
    self.nodeMethods.setNodeAttribute(attributes, 'isOutput', contents['output'])
    if contents['input'] or contents['output']: self.nodeMethods.setNodeAttribute(attributes, 'isFile', True)
    self.nodeMethods.setNodeAttribute(attributes, 'isRequired', contents['required'])
    if 'allow multiple values' in contents: self.nodeMethods.setNodeAttribute(attributes, 'allowMultipleValues', contents['allow multiple values'])

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

    return attributes

  # Set a value in the toolAttributes.
  def setToolAttribute(self, attributes, tool, attribute, value):
    try: test = getattr(attributes, attribute)

    # If the attribute can't be set, determine the source of the problem and provide an
    # error message.
    except:

      # If the tool is not available.
      if tool not in attributes: self.errors.invalidToolInSetToolAttribute(tool)

      # If the attribute being set is not valid.
      if attribute not in attributes: self.errors.invalidAttributeInSetToolAttribute(attribute)

    # Set the attribute.
    setattr(attributes, attribute, value)

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
