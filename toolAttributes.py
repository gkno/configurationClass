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
    self.inputIsStream = False
    self.isHidden      = None
    self.modifier      = None
    self.path          = None
    self.precommand    = None

class argumentAttributes:
  def __init__(self):
    self.allowMultipleValues      = False
    self.commandLineArgument      = None
    self.constructionInstructions = None
    self.description              = None
    self.dataType                 = None
    self.extension                = None
    self.filenameExtensions       = None
    self.hideInHelp               = False
    self.includeOnCommandLine     = True
    self.inputStream              = False
    self.isFilenameStub           = False
    self.isInput                  = False
    self.isInputList              = False
    self.isOutput                 = False
    self.isRequired               = False
    self.longFormArgument         = None
    self.modifyArgument           = None
    self.outputStream             = False
    self.repeatArgument           = None
    self.replaceArgument          = None
    self.shortFormArgument        = None

class toolConfiguration:
  def __init__(self):

    # Define the attributes for a tool.
    self.attributes           = {}

    # Define a structure to hold all the information about a tools arguments.
    self.argumentAttributes = {}

    # Define dictionaries to store the long and short form arguments.
    self.longFormArguments  = {}
    self.shortFormArguments = {}

    # Define the errors class for handling errors.
    self.errors               = configurationClassErrors()

    self.availableTools       = {}
    self.configurationData    = {}
    self.filename             = None

  # Process the tool data.
  def processConfigurationData(self, tool, data):

    # Include the tool in the list of available tools.
    self.availableTools[tool] = tool

    # Parse the tool configuration file and check that all fields are valid. Ensure that there are
    # no errors, omissions or inconsistencies. Store the information in the relevant data structures
    # as the checks are performed.
    #
    # Check the general tool information.
    self.attributes[tool] = self.checkGeneralAttributes(tool, data)

    # Check the validity of all of the supplied arguments.
    self.checkToolArguments(tool, data['arguments'])

    # Check general and argument attribute dependencies.
    self.checkAttributeDependencies(tool)

    # Generate a dictionary that links the long and short form arguments with each other..
    self.consolidateArguments(tool)

    # Look to see if the 'argument order' section is present and check its validity.
    self.checkArgumentOrder(tool, self.attributes[tool])

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
    allowedAttributes['input is stream']    = (bool, False, True, 'inputIsStream')
    allowedAttributes['instances']          = (list, True, False, None)
    allowedAttributes['modifier']           = (str, False, True, 'modifier')
    allowedAttributes['path']               = (str, True, True, 'path')
    allowedAttributes['precommand']         = (str, False, True, 'precommand')

    # Keep track of the observed required values.
    observedAttributes = {}

    # Loop over all of the attributes in the configuration file.
    for attribute in data:

      # If the value is not in the allowedAttributes, it is not an allowed value and execution
      # should be terminate with an error.
      if attribute not in allowedAttributes: self.errors.invalidGeneralAttributeInConfigurationFile(tool, attribute, allowedAttributes, False)

      # Mark this values as having been observed,
      observedAttributes[attribute] = True

      # Check that the value given to the attribute is of the correct type. If the value is unicode,
      # convert to a string first.
      value = str(data[attribute]) if isinstance(data[attribute], unicode) else data[attribute]
      if allowedAttributes[attribute][0] != type(value):
        self.errors.incorrectTypeInToolConfigurationFile(tool, attribute, None, value, allowedAttributes[attribute][0], False)

      # At this point, the attribute in the configuration file is allowed and of valid type. Check that 
      # the value itself is valid (if necessary) and store the value.
      if allowedAttributes[attribute][2]: self.setAttribute(attributes, tool, allowedAttributes[attribute][3], value)

    # Having parsed all of the general attributes attributes, check that all those that are required
    # are present.
    for attribute in allowedAttributes:
      if allowedAttributes[attribute][1] and attribute not in observedAttributes:
        self.errors.missingGeneralAttributeInConfigurationFile(tool, attribute, allowedAttributes, False)

    return attributes

  # Check that all the supplied arguments are valid and complete.
  def checkToolArguments(self, tool, arguments):

    # Initialise the data structure for this tool.
    self.argumentAttributes[tool] = {}

    # Keep track of the short from arguments for this tool.
    observedShortForms = {}

    # Define the allowed attributes. The structure describes the expected data type, whether the
    # attribute is requred and finally, the name of the attribute in the data structure storing the
    # values.
    allowedAttributes                                         = {}
    allowedAttributes['allow multiple values']                = (bool, False, 'allowMultipleValues')
    allowedAttributes['apply by repeating this argument']     = (str, False, 'repeatArgument')
    allowedAttributes['command line argument']                = (str, True, 'commandLineArgument')
    allowedAttributes['construct filename']                   = (dict, False, 'constructionInstructions')
    allowedAttributes['data type']                            = (str, True, 'dataType')
    allowedAttributes['description']                          = (str, True, 'description')
    allowedAttributes['extension']                            = (str, True, 'extension')
    allowedAttributes['filename extensions']                  = (list, False, 'filenameExtensions')
    allowedAttributes['hide in help']                         = (bool, False, 'hideInHelp')
    allowedAttributes['if input is stream']                   = (str, False, 'inputStream')
    allowedAttributes['if output to stream']                  = (str, False, 'outputStream')
    allowedAttributes['include on command line']              = (bool, False, 'includeOnCommandLine')
    allowedAttributes['input']                                = (bool, True, 'isInput')
    allowedAttributes['is filename stub']                     = (bool, False, 'isFilenameStub')
    allowedAttributes['list of input files']                  = (bool, False, 'isInputList')
    allowedAttributes['long form argument']                   = (str, True, 'longFormArgument')
    allowedAttributes['modify argument name on command line'] = (str, False, 'modifyArgument')
    allowedAttributes['output']                               = (bool, True, 'isOutput')
    allowedAttributes['replace argument with']                = (dict, False, 'replaceArgument')
    allowedAttributes['required']                             = (bool, True, 'isRequired')
    allowedAttributes['short form argument']                  = (str, False, 'shortFormArgument')

    for argumentDescription in arguments:

      # Keep track of the observed attributes.
      observedAttributes = {}

      # First check that the argument defines a dictionary of values.
      if not isinstance(argumentDescription, dict): self.errors.toolArgumentHasNoDictionary(tool)

      # First get the 'long form' for this argument. This will be used to identify the argument in error messages and
      # will be used as the key when storing attributes in a dictionary.
      try: longForm = argumentDescription['long form argument']
      except: self.errors.noLongFormForToolArgument(tool)

      # Check that this argument is unique.
      if longForm in self.argumentAttributes[tool]: self.errors.repeatedToolArgumentInToolConfigurationFile(tool, longForm, isLongForm = True)

      # Initialise the data structure for holding the argument information.
      attributes = argumentAttributes()

      # Store the long and short form arguments. If these aren't included, the routine will fail at the final check
      # since these are required argument. If the value is already included, fail.
      if 'short form argument' in argumentDescription:
        shortForm  = argumentDescription['short form argument']
        if shortForm in observedShortForms: self.errors.repeatedToolArgumentInToolConfigurationFile(tool, shortForm, isLongForm = False)
        else: observedShortForms[shortForm] = True

      # Loop over all entries in the argument description, checking that the attributes are allowed and valid.
      for attribute in argumentDescription:
        if attribute not in allowedAttributes:
          self.errors.invalidArgumentAttributeInToolConfigurationFile(tool, longForm, attribute, allowedAttributes)

        # Mark the attribute as observed.
        observedAttributes[attribute] = True

        # Check that the value given to the attribute is of the correct type. If the value is unicode,
        # convert to a string first.
        value = str(argumentDescription[attribute]) if isinstance(argumentDescription[attribute], unicode) else argumentDescription[attribute]
        if allowedAttributes[attribute][0] != type(value):
          self.errors.incorrectiTypeInConfigurationFile(tool, attribute, longForm, value, allowedAttributes[attribute][0])

        # Store the information in the attributes structure.
        self.setAttribute(attributes, tool, allowedAttributes[attribute][2], value)

      # Check if any required arguments are missing.
      for attribute in allowedAttributes:
        if allowedAttributes[attribute][1] and attribute not in observedAttributes:
          self.errors.missingArgumentAttributeInToolConfigurationFile(tool, longForm, attribute, allowedAttributes)

      # Store the attributes.
      self.argumentAttributes[tool][longForm] = attributes

  # Check all argument attribute dependencies.
  def checkAttributeDependencies(self, tool):

    # Define any dependencies. For each attribute, a list is provided allowing for different values associated
    # with the attribute being defined. It may be that there are different dependencies depending on the value
    # that the attribute takes. The format of the dictionary is as follows:
    #
    # (A, B, [])
    #
    # A: Is the value a general or argument attribute.
    # B: If defined, does the attribute require a value or just needs to be present. This takes the values 'present' or
    # the value that is being checked.
    dependencies = {}
    dependencies['inputIsStream'] = [('general', 'present', 'any', [('argument', 'includeOnCommandLine', False)])]
    dependencies['inputStream']   = [('argument', True)]

  # Generate a dictionary that links the long and short form arguments with each other.
  def consolidateArguments(self, tool):
    self.longFormArguments[tool]  = {}
    self.shortFormArguments[tool] = {}
    for longForm in self.argumentAttributes[tool]:
      shortForm = self.getArgumentAttribute(tool, longForm, 'shortFormArgument')
      self.longFormArguments[tool][longForm]   = shortForm
      self.shortFormArguments[tool][shortForm] = longForm

  # If the order in which the arguments should be used is included, check that all of the arguments are
  # included in the list and no invalid arguments are present.
  def checkArgumentOrder(self, tool, attributes):

    # If this tool does not include an argument order, the following checks are not required.
    if not attributes.argumentOrder: return

    # Loop over all of the arguments and check that they are represented in the argument order.
    for argument in self.argumentAttributes[tool]:
      if argument not in attributes.argumentOrder:
        self.errors.missingArgumentInArgumentOrder(tool, argument)

    # Loop over all of the arguments in the argument order and check that no arguments are invalid or repeated.
    observedArguments = []
    for argument in attributes.argumentOrder:
      if argument not in self.argumentAttributes[tool]: self.errors.invalidArgumentInArgumentOrder(tool, argument)
      if argument in observedArguments: self.errors.repeatedArgumentInArgumentOrder(tool, argument)
      observedArguments.append(argument)

  # TODO
  # Check all of the instance information.
  def checkInstanceInformation(self, tool, instances):
    pass

  # Get a tool argument attribute.
  def getGeneralAttribute(self, tool, attribute):
    try: value = getattr(self.attributes[tool], attribute)
    except:

      # Identify the source of the error.
      if tool not in self.attributes: self.errors.invalidToolInGeneralToolAttributes(tool, attribute)
      else: return None

    return value

  # Get all of the arguments for a tool.
  def getArguments(self, tool):
    arguments = []

    # If the supplied tool is invalid.
    if tool not in self.argumentAttributes: self.errors.invalidToolInGetArguments(tool)

    # Find all the arguments.
    for argument in self.argumentAttributes[tool]: arguments.append(argument)
    return arguments

  # Get a tool argument attribute.
  def getArgumentAttribute(self, tool, argument, attribute):
    try: value = getattr(self.argumentAttributes[tool][argument], attribute)
    except:

      # Identify the source of the error.
      if tool not in self.argumentAttributes:
        self.errors.invalidToolInToolArgumentAttributes(tool, argument, attribute, problemID = 'tool')
      elif argument not in self.argumentAttributes[tool]:
        self.errors.invalidToolInToolArgumentAttributes(tool, argument, attribute, problemID = 'argument')
      else: return None

    return value

  # Set a value in the toolAttributes.
  def setAttribute(self, attributes, tool, attribute, value):
    try: test = getattr(attributes, attribute)

    # If the attribute can't be set, determine the source of the problem and provide an
    # error message.
    except:

      # If the tool is not available.TODO
      self.errors.invalidAttributeInSetAttribute(attribute, False)
      self.errors.terminate()

    # Set the attribute.
    setattr(attributes, attribute, value)

    return attributes

  # Get the long form of a tool argument.
  def getLongFormArgument(self, tool, argument):

    # Check that the tool is valid.
    if tool not in self.longFormArguments: self.errors.missingToolInGetLongFormArgument(tool)

    # If the argument is already in its long form, return the argument.
    if argument in self.longFormArguments[tool]: return argument

    # Check if the argument is in the short form.
    if argument in self.shortFormArguments[tool]: return self.shortFormArguments[tool][argument]

    # If this argument is in neither of the previous dictionaries, the argument is not valid for this tool.
    # were the supplied argument, the argument is not valid for this tool.
    self.errors.unknownToolArgument(tool, argument)
