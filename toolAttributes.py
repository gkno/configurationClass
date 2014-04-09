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

    # Define the arguments associated with the tool.
    self.arguments = {}

    # Define the order in which the argument should be written.
    self.argumentOrder = []

    # Define the delimiter between the argument and the value on the
    # tool commmand line. This is usually a space.
    self.delimiter = ' '

    # A description of the tool.
    self.description = None

    # The category to which the tool belongs.
    self.category = None

    # The tool executable and its path and any modifiers.
    self.executable = None
    self.modifier   = None
    self.path       = None
    self.precommand = None

    # Record if the input to this tool needs to be a stream.
    self.inputIsStream = False

    # Record if this tool is hidden in the help.
    self.isHidden = False

    # Some tools do not produce any outputs. If this is the case, the tool has to
    # be marked.
    self.noOutput = False

    # Store the tools that need to be compiled for this tool to be available.
    self.requiredCompiledTools = []

class argumentAttributes:
  def __init__(self):

    # If an argument is allowed to be set multiple times.
    self.allowMultipleValues      = False

    # Store the defined long and short form of the argument recognised as well
    # as the argument expected by the tool.
    self.commandLineArgument = None
    self.longFormArgument    = None
    self.shortFormArgument   = None

    # Define the extensions allowed for the argument.
    self.extensions = []

    # Store instructions on how to construct the filename
    self.constructionInstructions = None

    # Record the argument description.
    self.description = None

    # Store the data type of the value supplied with this argument.
    self.dataType = None

    # Record id the argument points to a filename stub and store the 
    # associated extensions.
    self.filenameExtensions = None
    self.isFilenameStub     = False

    # Record if this argument should be hidden in the help.
    self.hideInHelp = False

    self.inputStream              = False
    self.isDirectory              = False
    self.isInput                  = False
    self.isInputList              = False
    self.isOutput                 = False
    self.isRequired               = False
    self.modifyArgument           = None
    self.outputStream             = False
    self.repeatArgument           = None
    self.replaceArgument          = None

    # Record if the presence of the specified file/directory is not allowed prior
    # to execution.
    self.terminateIfPresent = False

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

    # Define a variable to determine whether termination should result if an error in a
    # configuration file is found.
    self.allowTermination = True

    # TODO ARE THESE NEEDED?
    self.availableTools       = {}
    self.configurationData    = {}
    self.filename             = None

  # Process the tool data.
  def processConfigurationData(self, tool, data, allowTermination):

    # Set the allowTermination variable. Each of the following subroutines check different
    # aspects of the configuration file. If problems are found, termination will result with
    # an error message unless allowTermination is set to False.
    self.allowTermination = allowTermination
    success               = True

    # Include the tool in the list of available tools.
    self.availableTools[tool] = tool

    # Parse the tool configuration file and check that all fields are valid. Ensure that there are
    # no errors, omissions or inconsistencies. Store the information in the relevant data structures
    # as the checks are performed.
    #
    # Check the general tool information.
    success, self.attributes[tool] = self.checkGeneralAttributes(tool, data)

    # Check the validity of all of the supplied arguments.
    if success: success = self.checkToolArguments(tool, data['arguments'])

    # Check general and argument attribute dependencies.
    if success: success = self.checkAttributeDependencies(tool)

    # Generate a dictionary that links the long and short form arguments with each other..
    if success: success = self.consolidateArguments(tool)

    # Look to see if the 'argument order' section is present and check its validity.
    if success: success = self.checkArgumentOrder(tool, self.attributes[tool])

    # If filename constuction instructions are provided, check that all is provided.
    if success: success = self.checkConstructionInstructions(tool)

    return success

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
    allowedAttributes['category']           = (str, True, True, 'category')
    allowedAttributes['description']        = (str, True, True, 'description')
    allowedAttributes['executable']         = (str, True, True, 'executable')
    allowedAttributes['help']               = (str, True, False, None)
    allowedAttributes['hide tool']          = (bool, False, True, 'isHidden')
    allowedAttributes['input is stream']    = (bool, False, True, 'inputIsStream')
    allowedAttributes['instances']          = (list, True, False, None)
    allowedAttributes['modifier']           = (str, False, True, 'modifier')
    allowedAttributes['path']               = (str, True, True, 'path')
    allowedAttributes['precommand']         = (str, False, True, 'precommand')
    allowedAttributes['no output']          = (bool, False, True, 'noOutput')
    allowedAttributes['tools']              = (list, True, True, 'requiredCompiledTools')

    # Keep track of the observed required values.
    observedAttributes = {}

    # Loop over all of the attributes in the configuration file.
    for attribute in data:

      # If the value is not in the allowedAttributes, it is not an allowed value and execution
      # should be terminate with an error.
      if attribute not in allowedAttributes:
        if self.allowTermination: self.errors.invalidGeneralAttributeInConfigurationFile(tool, attribute, allowedAttributes, False)
        else: return False, attributes

      # Mark this values as having been observed,
      observedAttributes[attribute] = True

      # Check that the value given to the attribute is of the correct type. If the value is unicode,
      # convert to a string first.
      value = str(data[attribute]) if isinstance(data[attribute], unicode) else data[attribute]
      if allowedAttributes[attribute][0] != type(value):
        if self.allowTermination:
          self.errors.incorrectTypeInToolConfigurationFile(tool, attribute, None, value, allowedAttributes[attribute][0], False)
        else: return False, attributes

      # At this point, the attribute in the configuration file is allowed and of valid type. Check that 
      # the value itself is valid (if necessary) and store the value.
      if allowedAttributes[attribute][2]: self.setAttribute(attributes, tool, allowedAttributes[attribute][3], value)

    # Having parsed all of the general attributes attributes, check that all those that are required
    # are present.
    for attribute in allowedAttributes:
      if allowedAttributes[attribute][1] and attribute not in observedAttributes:
        if self.allowTermination: self.errors.missingGeneralAttributeInConfigurationFile(tool, attribute, allowedAttributes, False)
        else: return False, attributes

    return True, attributes

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
    allowedAttributes['directory']                            = (bool, False, 'isDirectory')
    allowedAttributes['extensions']                           = (list, True, 'extensions')
    allowedAttributes['filename extensions']                  = (list, False, 'filenameExtensions')
    allowedAttributes['hide in help']                         = (bool, False, 'hideInHelp')
    allowedAttributes['if input is stream']                   = (str, False, 'inputStream')
    allowedAttributes['if output to stream']                  = (str, False, 'outputStream')
    allowedAttributes['input']                                = (bool, True, 'isInput')
    allowedAttributes['is filename stub']                     = (bool, False, 'isFilenameStub')
    allowedAttributes['list of input files']                  = (bool, False, 'isInputList')
    allowedAttributes['long form argument']                   = (str, True, 'longFormArgument')
    allowedAttributes['modify argument name on command line'] = (str, False, 'modifyArgument')
    allowedAttributes['output']                               = (bool, True, 'isOutput')
    allowedAttributes['replace argument with']                = (dict, False, 'replaceArgument')
    allowedAttributes['required']                             = (bool, True, 'isRequired')
    allowedAttributes['short form argument']                  = (str, False, 'shortFormArgument')
    allowedAttributes['terminate if present']                 = (bool, False, 'terminateIfPresent')

    for argumentDescription in arguments:

      # Keep track of the observed attributes.
      observedAttributes = {}

      # First check that the argument defines a dictionary of values.
      if not isinstance(argumentDescription, dict): self.errors.toolArgumentHasNoDictionary(tool)

      # First get the 'long form' for this argument. This will be used to identify the argument in error messages and
      # will be used as the key when storing attributes in a dictionary.
      try: longFormArgument = argumentDescription['long form argument']
      except:
        if self.allowTermination: self.errors.noLongFormForToolArgument(tool)
        else: return False

      # Check that this argument is unique.
      if longFormArgument in self.argumentAttributes[tool]:
        if self.allowTermination: self.errors.repeatedToolArgumentInToolConfigurationFile(tool, longFormArgument, isLongForm = True)
        else: return False

      # Initialise the data structure for holding the argument information.
      attributes = argumentAttributes()

      # Store the long and short form arguments. If these aren't included, the routine will fail at the final check
      # since these are required argument. If the value is already included, fail.
      if 'short form argument' in argumentDescription:
        shortFormArgument = argumentDescription['short form argument']
        if shortFormArgument in observedShortForms:
          if self.allowTermination: self.errors.repeatedToolArgumentInToolConfigurationFile(tool, shortFormArgument, isLongForm = False)
          else: return False
        else: observedShortForms[shortFormArgument] = True

      # Loop over all entries in the argument description, checking that the attributes are allowed and valid.
      for attribute in argumentDescription:
        if attribute not in allowedAttributes:
          if self.allowTermination: self.errors.invalidArgumentAttributeInToolConfigurationFile(tool, longFormArgument, attribute, allowedAttributes)
          else: return False

        # Mark the attribute as observed.
        observedAttributes[attribute] = True

        # Check that the value given to the attribute is of the correct type. If the value is unicode,
        # convert to a string first.
        value = str(argumentDescription[attribute]) if isinstance(argumentDescription[attribute], unicode) else argumentDescription[attribute]
        if allowedAttributes[attribute][0] != type(value):
          if self.allowTermination:
            self.errors.incorrectiTypeInConfigurationFile(tool, attribute, longFormArgument, value, allowedAttributes[attribute][0])
          else: return False

        # Store the information in the attributes structure.
        self.setAttribute(attributes, tool, allowedAttributes[attribute][2], value)

      # Check if any required arguments are missing.
      for attribute in allowedAttributes:
        if allowedAttributes[attribute][1] and attribute not in observedAttributes:
          if self.allowTermination: self.errors.missingArgumentAttributeInToolConfigurationFile(tool, longFormArgument, attribute, allowedAttributes)
          else: return False

      # Store the attributes.
      self.argumentAttributes[tool][longFormArgument] = attributes

    return True

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

    return True

  # Generate a dictionary that links the long and short form arguments with each other.
  def consolidateArguments(self, tool):
    self.longFormArguments[tool]  = {}
    self.shortFormArguments[tool] = {}
    for longForm in self.argumentAttributes[tool]:
      shortForm = self.getArgumentAttribute(tool, longForm, 'shortFormArgument')
      self.longFormArguments[tool][longForm]   = shortForm
      self.shortFormArguments[tool][shortForm] = longForm

    return True

  # If the order in which the arguments should be used is included, check that all of the arguments are
  # included in the list and no invalid arguments are present.
  def checkArgumentOrder(self, tool, attributes):

    # If this tool does not include an argument order, the following checks are not required.
    if not attributes.argumentOrder: return True

    # Loop over all of the arguments and check that they are represented in the argument order.
    for argument in self.argumentAttributes[tool]:
      if argument not in attributes.argumentOrder:
        if self.allowTermination: self.errors.missingArgumentInArgumentOrder(tool, argument)
        else: return False

    # Loop over all of the arguments in the argument order and check that no arguments are invalid or repeated.
    observedArguments = []
    for argument in attributes.argumentOrder:
      if argument not in self.argumentAttributes[tool]: self.errors.invalidArgumentInArgumentOrder(tool, argument)
      if argument in observedArguments:
        if self.allowTermination: self.errors.repeatedArgumentInArgumentOrder(tool, argument)
        else: return False
      observedArguments.append(argument)

    return True

  # Check that filename constructions are valid and complete.
  def checkConstructionInstructions(self, tool):
    allowedMethods = []
    allowedMethods.append('define name')
    allowedMethods.append('from tool argument')

    for argument in self.argumentAttributes[tool]:
      if self.argumentAttributes[tool][argument].constructionInstructions:

        # For each allowed method, check that everything required is present. First get the method.
        instructions = self.argumentAttributes[tool][argument].constructionInstructions
        if 'method' not in instructions: self.errors.noConstructionMethod(tool, argument, allowedMethods)

        # Now check the specifics of each method.
        method = self.argumentAttributes[tool][argument].constructionInstructions['method']
        if method == 'define name': success = self.checkDefineName(tool, argument)
        elif method == 'from tool argument': success = self.checkFromToolArgument(tool, argument)
        else:
          if self.allowTermination: self.errors.unknownConstructionMethod(tool, argument, method, allowedMethods)
          else: return False

    return True

  # Check constructions instructions for the 'define name' method.
  def checkDefineName(self, tool, argument):
    allowedAttributes                                 = {}
    allowedAttributes['add extension']                = (bool, True)
    allowedAttributes['directory argument']           = (str, False)
    allowedAttributes['filename']                     = (str, True)
    allowedAttributes['for multiple runs connect to'] = (str, True)
    allowedAttributes['method']                       = (str, True)

    # Keep track of the observed required values.
    observedAttributes = {}

    # Loop over all of the attributes in the configuration file.
    for attribute in self.argumentAttributes[tool][argument].constructionInstructions:

      # If the value is not in the allowedAttributes, it is not an allowed value and execution
      # should be terminate with an error.
      if attribute not in allowedAttributes:
        if self.allowTermination: self.errors.invalidAttributeInConstruction(tool, argument, attribute, 'define name', allowedAttributes)
        else: return False

      # Mark this values as having been observed,
      observedAttributes[attribute] = True

      # Check that the value given to the attribute is of the correct type. If the value is unicode,
      # convert to a string first.
      value = self.argumentAttributes[tool][argument].constructionInstructions[attribute]
      value = str(value) if isinstance(value, unicode) else value
      if allowedAttributes[attribute][0] != type(value):
        if self.allowTermination:
          self.errors.incorrectTypeInConstruction(tool, argument, attribute, 'define name', value, allowedAttributes[attribute][0])
        else: return False

    # Having parsed all of the general attributes attributes, check that all those that are required
    # are present.
    for attribute in allowedAttributes:
      if allowedAttributes[attribute][1] and attribute not in observedAttributes and self.allowTermination: 
        self.errors.missingAttributeInConstruction(tool, argument, attribute, 'define name', allowedAttributes)

    # If the 'directory argument' was present, check that this is a valid  argument for this tool. Being
    # valid means that the argument exists and is a directory argument.
    if 'directory argument' in self.argumentAttributes[tool][argument].constructionInstructions:

      # Get all directory arguments for this tool.
      directoryArguments = []
      for addArgument in self.argumentAttributes[tool].keys():
        if self.argumentAttributes[tool][addArgument].isDirectory: directoryArguments.append(addArgument)

      # Check the validity of the entry in the configuration file.
      addArgument = self.argumentAttributes[tool][argument].constructionInstructions['directory argument']
      if addArgument not in directoryArguments:
        if self.allowTermination: self.errors.invalidArgumentInConstruction(tool, argument, addArgument, directoryArguments, 'directory argument')
        else: return False

    return True

  # Check constructions instructions for the 'from tool argument' method.
  def checkFromToolArgument(self, tool, argument):
    allowedAttributes                        = {}
    allowedAttributes['method']              = (str, True)
    allowedAttributes['modify extension']    = (str, True)
    allowedAttributes['use argument']        = (str, True)
    allowedAttributes['add additional text'] = (str, False)
    allowedAttributes['add argument values'] = (list, False)

    # Keep track of the observed required values.
    observedAttributes = {}

    # Loop over all of the attributes in the configuration file.
    for attribute in self.argumentAttributes[tool][argument].constructionInstructions:

      # If the value is not in the allowedAttributes, it is not an allowed value and execution
      # should be terminate with an error.
      if attribute not in allowedAttributes:
        if self.allowTermination: self.errors.invalidAttributeInConstruction(tool, argument, attribute, 'from tool argument', allowedAttributes)
        else: return False

      # Mark this values as having been observed,
      observedAttributes[attribute] = True

      # Check that the value given to the attribute is of the correct type. If the value is unicode,
      # convert to a string first.
      value = self.argumentAttributes[tool][argument].constructionInstructions[attribute]
      value = str(value) if isinstance(value, unicode) else value
      if allowedAttributes[attribute][0] != type(value):
        if self.allowTermination:
          self.errors.incorrectTypeInConstruction(tool, argument, attribute, 'from tool argument', value, allowedAttributes[attribute][0])
        else: return False

    # Having parsed all of the general attributes attributes, check that all those that are required
    # are present.
    for attribute in allowedAttributes:
      if allowedAttributes[attribute][1] and attribute not in observedAttributes:
        if self.allowTermination: self.errors.missingAttributeInConstruction(tool, argument, attribute, 'from tool argument', allowedAttributes)
        else: return False

    # If the 'add argument values' was present, check that this list contains valid arguments for this tool.
    if 'add argument values' in self.argumentAttributes[tool][argument].constructionInstructions:
      for addArgument in self.argumentAttributes[tool][argument].constructionInstructions['add argument values']:
        if addArgument not in self.argumentAttributes[tool]:
          if self.allowTermination:
            self.errors.invalidArgumentInConstruction(tool, argument, addArgument, self.argumentAttributes[tool].keys(), 'add argument values')
          else: return False

    return True

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
  def getLongFormArgument(self, tool, argument, allowTermination = True):

    # Check that the tool is valid.
    if tool not in self.longFormArguments: self.errors.missingToolInGetLongFormArgument(tool)

    # If the argument is already in its long form, return the argument.
    if argument in self.longFormArguments[tool]: return argument

    # Check if the argument is in the short form.
    if argument in self.shortFormArguments[tool]: return self.shortFormArguments[tool][argument]

    # If this argument is in neither of the previous dictionaries, the argument is not valid for this tool.
    # were the supplied argument, the argument is not valid for this tool.
    if allowTermination: self.errors.unknownToolArgument(tool, argument)
    else: return None

  # Get the method of filename construction.
  def getConstructionMethod(self, tool, argument):
    if self.argumentAttributes[tool][argument].constructionInstructions:
      return self.argumentAttributes[tool][argument].constructionInstructions['method']

    else: None

  # Get the information for the construction instructions.
  def getAttributeFromDefinedConstruction(self, tool, argument, attribute):
    try: value = self.argumentAttributes[tool]
    except: print('ERROR - configurationClass.tools.getAttributeFromDefinedConstruction'); self.errors.terminate()

    try: value = self.argumentAttributes[tool][argument]
    except: print('ERROR - configurationClass.tools.getAttributeFromDefinedConstruction'); self.errors.terminate()

    if attribute not in self.argumentAttributes[tool][argument].constructionInstructions:
      print('ERROR - configurationClass.tools.getAttributeFromDefinedConstruction')
      self.errors.terminate()

    else: return self.argumentAttributes[tool][argument].constructionInstructions[attribute]

  # Determine whether to add an extension when constructing the filename.
  def addExtensionFromConstruction(self, tool, argument):
    return self.argumentAttributes[tool][argument].constructionInstructions['add extension']
