#!/bin/bash/python

from __future__ import print_function
import networkx as nx
from copy import deepcopy

import configurationClassErrors
from configurationClassErrors import *

import json
import os
import sys

class toolAttributes:
  def __init__(self):
    self.arguments     = {}
    self.argumentOrder = []
    self.delimiter     = ' '
    self.description   = None
    self.executable    = None
    self.isHidden      = None
    self.modifier      = None
    self.path          = None
    self.precommand    = None

class argumentAttributes:
  def __init__(self):
    self.commandLineArgument      = None
    self.constructionInstructions = None
    self.description              = None
    self.dataType                 = None
    self.extension                = None
    self.hideInHelp               = False
    self.ID                       = None
    self.includeOnCommandLine     = True
    self.isInput                  = False
    self.isOutput                 = False
    self.longFormArgument         = None
    self.modifyArgument           = None
    self.required                 = False
    self.shortFormArgument        = None

class toolConfiguration:
  def __init__(self):
    self.attributes           = {}
    self.availableTools       = {}
    self.configurationData    = {}
    self.errors               = configurationClassErrors()
    self.filename             = None
    self.jsonError            = None
    self.setRequiredArguments = False

    # Define a structure to hold all the information about a tools arguments.
    self.argumentAttributes = {}

  # Process the tool data.
  def processConfigurationData(self, tool, data):

    # Include the tool in the list of available tools.
    self.availableTools[tool] = tool

    # Parse the tool configuration file and check that all fields are valid. Ensure that there are
    # no errors, omissions or inconsistencies. Store the information in the relevant data structures
    # as the checks are performed.
    #
    # Check the general tool information.
    attributes = self.checkGeneralAttributes(tool, data)

    # Check the validity of all of the supplied arguments.
    self.checkToolArguments(tool, data['arguments'])

    # Look to see if the 'argument order' section is present and check its validity.
    self.checkArgumentOrder(tool, attributes)

    # Push all of the attributes to the tool.
    self.attributes[tool] = attributes

    return data['instances']

  # Check and store the top level tool attibutes.
  def checkGeneralAttributes(self, tool, data):

    # Set the general tool attributes.
    attributes = toolAttributes()

    # Define the allowed values. The tuple contains the expected type and whether the
    # value is required, whether the value should be stored and finally the name in the
    # attributes data structure under which it should be stored..
    allowedAttributes                       = {}
    allowedAttributes['arguments']          = (list, True, False, None)
    allowedAttributes['argument delimiter'] = (str, False, True, 'delimiter')
    allowedAttributes['argument order']     = (list, False, True, 'argumentOrder')
    allowedAttributes['description']        = (str, True, True, 'description')
    allowedAttributes['executable']         = (str, True, True, 'executable')
    allowedAttributes['help']               = (str, True, False, None)
    allowedAttributes['hide tool']          = (bool, False, True, 'isHidden')
    allowedAttributes['instances']          = (dict, True, False, None)
    allowedAttributes['modifier']           = (str, False, True, 'modifier')
    allowedAttributes['path']               = (str, True, True, 'path')
    allowedAttributes['precommand']         = (str, False, True, 'precommand')

    # Keep track of the observed required values.
    observedAttributes = {}

    # Loop over all of the attributes in the configuration file.
    for attribute in data:

      # If the value is not in the allowedAttributes, it is not an allowed value and execution
      # should be terminate with an error.
      if attribute not in allowedAttributes: self.errors.invalidGeneralAttributeInToolConfigurationFile(tool, attribute, allowedAttributes)

      # Mark this values as having been observed,
      observedAttributes[attribute] = True

      # Check that the value given to the attribute is of the correct type. If the value is unicode,
      # convert to a string first.
      value = str(data[attribute]) if isinstance(data[attribute], unicode) else data[attribute]
      if allowedAttributes[attribute][0] != type(value):
        self.errors.incorrectTypeInToolConfigurationFile(tool, attribute, value, allowedAttributes[attribute][0], ID = None)

      # At this point, the attribute in the configuration file is allowed and of valid type. Check that 
      # the value itself is valid (if necessary) and store the value.
      if allowedAttributes[attribute][2]: self.setToolAttribute(attributes, tool, allowedAttributes[attribute][3], value)

    # Having parsed all of the general attributes attributes, check that all those that are required
    # are present.
    for attribute in allowedAttributes:
      if allowedAttributes[attribute][1] and attribute not in observedAttributes:
        self.errors.missingGeneralAttributeInToolConfigurationFile(tool, attribute, allowedAttributes)

    return attributes

  # Check that all the supplied arguments are valid and complete.
  def checkToolArguments(self, tool, arguments):

    # Initialise the data structure for this tool.
    self.argumentAttributes[tool] = {}

    # Keep track of the arguments (long and short forms) seen for this tool.
    observedLongForms  = {}
    observedShortForms = {}

    # Define the allowed attributes. The structure describes the expected data type, whether the
    # attribute is requred and finally, the name of the attribute in the data structure storing the
    # values.
    allowedAttributes                                         = {}
    allowedAttributes['command line argument']                = (str, True, 'commandLineArgument')
    allowedAttributes['construct filename']                   = (dict, False, 'constructionInstructions')
    allowedAttributes['data type']                            = (str, True, 'dataType')
    allowedAttributes['description']                          = (str, True, 'description')
    allowedAttributes['extension']                            = (str, True, 'extension')
    allowedAttributes['hide in help']                         = (bool, False, 'hideInHelp')
    allowedAttributes['ID']                                   = (str, True, 'ID')
    allowedAttributes['include on command line']              = (bool, False, 'includeOnCommandLine')
    allowedAttributes['input']                                = (bool, True, 'isInput')
    allowedAttributes['long form argument']                   = (str, True, 'longFormArgument')
    allowedAttributes['modify argument name on command line'] = (str, False, 'modifyArgument')
    allowedAttributes['output']                               = (bool, True, 'isOutput')
    allowedAttributes['required']                             = (bool, True, 'required')
    allowedAttributes['short form argument']                  = (str, False, 'shortFormArgument')

    for argumentDescription in arguments:

      # Keep track of the observed attributes.
      observedAttributes = {}

      # First check that the argument defines a dictionary of values.
      if not isinstance(argumentDescription, dict): self.errors.toolArgumentHasNoDictionary(tool)

      # First get the 'ID' for this argument. This will be used to identify the argument in error messages and
      # will be used as the key when storing attributes in a dictionary.
      try: ID = argumentDescription['ID']
      except: self.errors.noIDForToolArgument(tool)

      # Check that this ID is unique.
      if ID in self.argumentAttributes[tool]: self.errors.repeatedArgumentIDInToolConfigurationFile(tool, ID)

      # Initialise the data structure for holding the argument information.
      attributes = argumentAttributes()

      # Store the long and short form arguments. If these aren't included, the routine will fail at the final check
      # since these are required argument. If the value is already included, fail.
      if 'long form argument' in argumentDescription:
        longForm  = argumentDescription['long form argument']
        if longForm in observedLongForms: self.errors.repeatedToolArgumentInToolConfigurationFile(tool, ID, longForm, isLongForm = True)
        else: observedLongForms[longForm] = True
      if 'short form argument' in argumentDescription:
        shortForm  = argumentDescription['short form argument']
        if shortForm in observedShortForms: self.errors.repeatedToolArgumentInToolConfigurationFile(tool, ID, shortForm, isLongForm = False)
        else: observedShortForms[shortForm] = True

      # Loop over all entries in the argument description, checking that the attributes are allowed and valid.
      for attribute in argumentDescription:
        if attribute not in allowedAttributes: self.errors.invalidArgumentAttributeInToolConfigurationFile(tool, ID, attribute, allowedAttributes)

        # Mark the attribute as observed.
        observedAttributes[attribute] = True

        # Check that the value given to the attribute is of the correct type. If the value is unicode,
        # convert to a string first.
        value = str(argumentDescription[attribute]) if isinstance(argumentDescription[attribute], unicode) else argumentDescription[attribute]
        if allowedAttributes[attribute][0] != type(value):
          self.errors.incorrectTypeInToolConfigurationFile(tool, attribute, value, allowedAttributes[attribute][0], ID)

        # Store the information in the attributes structure.
        self.setToolAttribute(attributes, tool, allowedAttributes[attribute][2], value)

      # Check if any required arguments are missing.
      for attribute in allowedAttributes:
        if allowedAttributes[attribute][1] and attribute not in observedAttributes:
          self.errors.missingArgumentAttributeInToolConfigurationFile(tool, ID, attribute, allowedAttributes)

      # Store the attributes.
      self.argumentAttributes[tool][ID] = attributes

  # If the order in which the arguments should be used is included, check that all of the arguments are
  # included in the list and no invalid arguments are present.
  def checkArgumentOrder(self, tool, attributes):

    # If this tool does not include an argument order, the following checks are not required.
    if not attributes.argumentOrder: return

    # Loop over all of the arguments and check that they are represented in the argument order.
    for argumentID in self.argumentAttributes[tool]:
      if argumentID not in attributes.argumentOrder:
        self.errors.missingArgumentIDInArgumentOrder(tool, argumentID, self.getArgumentAttribute(tool, argumentID, 'longFormArgument'))

    # Loop over all of the argument in the argument order and check that no arguments are invalid or repeated.
    observedIDs = []
    for argumentID in attributes.argumentOrder:
      if argumentID not in self.argumentAttributes[tool]: self.errors.invalidArgumentIDInArgumentOrder(tool, argumentID)
      if argumentID in observedIDs: self.errors.repeatedArgumentIDInArgumentOrder(tool, argumentID)
      observedIDs.append(argumentID)

  # Get a tool argument attribute.
  def getArgumentAttribute(self, tool, argumentID, attribute):
    try: value = getattr(self.argumentAttributes[tool][argumentID], attribute)
    except:

      # Identify the source of the error.
      if tool not in self.argumentAttributes:
        self.errors.invalidToolInArgumentAttributes(tool, argumentID, attribute, problemID = 'tool')
      elif argumentID not in self.argumentAttributes[tool]:
        self.errors.invalidToolInArgumentAttributes(tool, argumentID, attribute, problemID = 'ID')
      else: return None

    return value









  # TODO CHECK THIS. GET INFO USING getToolAttribute.
  # Get information from the configuration file (not argument data).
  def getConfigurationData(self, tool, attribute):
    try: value = self.configurationData[tool][attribute]
    except:

      #FIXME
      if tool not in self.configurationData:
        print('MISSING TOOL: tools.getConfigurationData', tool)
        self.errors.terminate()
  
      # If the attribute cannot be found, return None.
      # TODO Include a check that the attribute is valid. Want to return None for
      # cases where attribute is allowed, but not present (e.g. precommand).
      if attribute not in self.configurationData[tool]: return None

    return value
 
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
        for argument in self.configurationData[tool]['arguments']: print(argument)
        self.errors.terminate()

      if attribute not in self.configurationData[tool]['arguments'][argument]:
        return None

    return value

  # Set a value in the toolAttributes.
  def setToolAttribute(self, attributes, tool, attribute, value):
    try: test = getattr(attributes, attribute)

    # If the attribute can't be set, determine the source of the problem and provide an
    # error message.
    except:

      # If the tool is not available.TODO
      self.errors.invalidAttributeInSetToolAttribute(attribute)
      self.errors.terminate()

    # Set the attribute.
    setattr(attributes, attribute, value)

    return attributes

  # Get the long form of a tool argument.
  def getLongFormArgument(self, tool, argument):
    try: value = self.configurationData[tool]['arguments'][argument]['shortForm']
    except:

      #FIXME Sort all the errors.
      if tool not in self.configurationData:
        print('MISSING TOOL: tools.getLongFormArgument', tool)
        self.errors.terminate()

      # If the argument is not in the configurationData structure, this might be because
      # the short form of the argument was supplied.
      if argument not in self.configurationData[tool]['arguments']:
        for toolArgument in self.configurationData[tool]['arguments']:
          shortForm = self.getArgumentData(tool, toolArgument, 'short form argument')
          if shortForm == argument: return toolArgument

        # If all the short form arguments for this tool were searched and none of them
        # were the supplied argument, the argument is not valid for this tool.
        self.errors.unknownToolArgument(tool, argument)
        argument = None

    # If the supplied argument was already the long form version, return the original
    # argument.
    return argument
