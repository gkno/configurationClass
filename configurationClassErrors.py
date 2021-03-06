#!/usr/bin/python

from __future__ import print_function

import inspect
from inspect import currentframe, getframeinfo, getouterframes

import os
import sys

class configurationClassErrors:

  # Initialise.
  def __init__(self):
    self.hasError  = False
    self.errorType = 'ERROR'
    self.text      = []

    # Store if in debugging mode.
    self.isDebug = False

  # Format the error message and write to screen.
  def writeFormattedText(self):
      firstLine = True
      secondLine = False
      maxLength = 93 - 5
      print(file = sys.stderr)
      for line in self.text:
        textList = []
        while len(line) > maxLength:
          index = line.rfind(' ', 0, maxLength)
          if index == -1:
            index = line.find(' ', 0, len(line))
            if index == -1:
              textList.append(line)
              line = ''
            else:
              textList.append(line[0:index])
              line = line[index + 1:len(line)]
          else:
            textList.append(line[0:index])
            line = line[index + 1:len(line)]

        if line != '' and line != ' ': textList.append(line)
        line = textList.pop(0)
        while line.startswith(' '): line = line[1:]

        if firstLine and self.errorType == 'ERROR':
          print('ERROR:   %-*s' % (1, line), file=sys.stderr)
        elif firstLine and self.errorType == 'WARNING':
          print('WARNING:   %-*s' % (1, line), file=sys.stderr)
        elif secondLine:
          print('DETAILS: %-*s' % (1, line), file=sys.stderr)
          secondLine = False
        else:
          print('         %-*s' % (1, line), file=sys.stderr)
        for line in textList:
          while line.startswith(' '): line = line[1:]

          if secondLine: print('DETAILS: %-*s' % (1, line), file=sys.stderr)
          else: print('         %-*s' % (1, line), file=sys.stderr)

        if firstLine:
          print(file=sys.stderr)
          firstLine = False
          secondLine = True

  #############################################
  # Errors with handling configuration files. #
  #############################################

  # If a configuration file cannot be found.
  def missingFile(self, filename):
    self.text.append('Missing configuration file.')
    self.text.append('The file \'' + filename + '\' could not be located."')
    self.writeFormattedText()
    self.terminate()

  # If there are errors with decoding the json file.
  def jsonError(self, error, filename):
    self.text.append('Malformed json file: ' + filename)
    self.text.append(str(error) + '.')
    self.writeFormattedText()
    self.terminate()

  #######################################################################
  # Errors with configuration files (common to both tool and pipeline). #
  #######################################################################

  # A general entry in the configuration file is invalid.
  def invalidGeneralAttributeInConfigurationFile(self, name, attribute, allowedAttributes, isPipeline):
    runType = 'pipeline' if isPipeline else 'tool'
    self.text.append('Invalid attribute in ' + runType + ' configuration file: ' + attribute)
    text = 'The configuration file for ' + runType + ' \'' + name + '\' contains the general attribute \'' + attribute + '\'. This is an ' + \
    'unrecognised attribute which is not permitted. The general attributes allowed in a ' + runType + ' configuration file are:'
    self.text.append(text)
    self.text.append('\t')

    # Create a sorted list of the allowed attributes.
    allowed = []
    for attribute in allowedAttributes: allowed.append(attribute)

    # Add the attributes to the text to be written along with the expected type.
    for attribute in sorted(allowed):
      self.text.append(attribute + ':\t' + str(allowedAttributes[attribute][0]) + ', required = ' + str(allowedAttributes[attribute][1]))

    self.text.append('\t')
    self.text.append('Please remove or correct the invalid attribute in the configuration file.')
    self.writeFormattedText()
    self.terminate()

  # A general entry in the configuration file is missing.
  def missingGeneralAttributeInConfigurationFile(self, name, attribute, allowedAttributes, isPipeline):
    runType = 'pipeline' if isPipeline else 'tool'

    self.text.append('Missing attribute in ' + runType + ' configuration file: ' + attribute)
    text = 'The configuration file for ' + runType + ' \'' + name + '\' is missing the general attribute \'' + attribute + '\'. The following ' + \
    'general attributes are required in a ' + runType + ' configuration file:'
    self.text.append(text)
    self.text.append('\t')

    # Create a sorted list of the required attributes.
    requiredAttributes = []
    for attribute in allowedAttributes:
      if allowedAttributes[attribute][1]: requiredAttributes.append(attribute)

    # Add the attributes to the text to be written along with the expected type.
    for attribute in sorted(requiredAttributes): self.text.append(attribute + ':\t' + str(allowedAttributes[attribute][0]))

    self.text.append('\t')
    self.text.append('Please add the missing attribute to the configuration file.')
    self.writeFormattedText()
    self.terminate()

  # A task included in the 'originating edges' section is invalid.
  def invalidTaskInOriginatingEdges(self, configNodeID, task):
    self.text.append('Invalid task in originating edges section.')
    self.text.append('The pipeline configuration file contains a node with ID \'' + configNodeID + '\' with a set of edges defined in the \'' + \
    'originating edges\' section. The task \'' + task + '\' appears in this list, but this is not a valid task in the pipeline. Please correct ' + \
    'the pipeline configuration file to fix this problem.')
    self.writeFormattedText()
    self.terminate()

  # A task included in the 'additional nodes' section is invalid.
  def invalidTaskInAdditionalNodes(self, configNodeID, task):
    self.text.append('Invalid task in additional nodes section.')
    self.text.append('The pipeline configuration file contains a node with ID \'' + configNodeID + '\' with a set of nodes defined in the \'' + \
    'additional nodes\' section. The task \'' + task + '\' appears in this list, but this is not a valid task in the pipeline. Please correct ' + \
    'the pipeline configuration file to fix this problem.')
    self.writeFormattedText()
    self.terminate()

  # A task included in the 'additional nodes' section has tis invalid.
  def repeatedArgumentInAdditionalNodes(self, configNodeID, task, argument):
    self.text.append('Invalid task in additional nodes section.')
    self.text.append('The pipeline configuration file contains a node with ID \'' + configNodeID + '\' with a set of nodes defined in the \'' + \
    'additional nodes\' section. The task \'' + task + '\' appears in this list, but this is not a valid task in the pipeline. Please correct ' + \
    'the pipeline configuration file to fix this problem.')
    self.writeFormattedText()
    self.terminate()

  # A pipeline node has no connections.
  def nodeHasNoConnections(self, configNodeID):
    self.text.append('Pipeline node has no connections.')
    self.text.append('The pipeline configuration file contains a node with ID \'' + configNodeID + '\'. This node does not contain any ' + \
    'connections. All pipeline nodes must be connected to at least one graph node, defined using either the \'tasks\', \'greedy tasks\' or ' + \
    'the \'originating edges\' fields. Please see the documentation on pipeline configuration files and repair the pipeline configuration file.')
    self.writeFormattedText()
    self.terminate()

  # A task included in the 'originating edges' section is associated with an invalid argument.
  def invalidArgumentInOriginatingEdges(self, task, tool, argument, configNodeID):
    self.text.append('Invalid argument in originating edges section.')
    self.text.append('The pipeline configuration file contains a node (ID: ' + configNodeID + ') with a set of edges defined in the \'' + \
    'originating edges\' section. Contained in this list is the task \'' + task + '\' associated with the argument \'' + argument + '\', but ' + \
    'this in an invalid arguent for the tool \'' + tool + '\' used by this task. Please check and fix the pipeline configutaion file.')
    self.writeFormattedText()
    self.terminate()

  # If the category/help group is invalid.
  def invalidCategory(self, runName, category, allowedCategories, isPipeline):
    runType = 'pipeline' if isPipeline else 'tool'

    self.text.append('Invalid category in ' + runType + ' configuration file.')
    text = 'The configuration file for ' + runType + ' \'' + runName + '\' includes the category \'' + category + '\', but this is not ' + \
    'a valid category for the ' + runType + '. The valid categories are:'
    self.text.append(text)
    self.text.append('\t')

    # Add the attributes to the text to be written along with the expected type.
    for attribute in sorted(allowedCategories): self.text.append(attribute)

    self.text.append('\t')
    self.text.append('Please replace the category with a valid value in the configuration file.')
    self.writeFormattedText()
    self.terminate()

  ##########################################
  # Errors with a tool configuration file. #
  ##########################################

  # Attribute has the wrong type.
  def incorrectTypeInToolConfigurationFile(self, tool, group, attribute, argument, value, expectedType):

    # Find the given type.
    isType    = self.findType(type(value))
    needsType = self.findType(expectedType)
    if isType == None: isType = 'Unknown'
    if needsType == None: needsType = 'Unknown'
    self.text.append('Invalid attribute value in tool configuration file.')
    text = 'The attribute \'' + str(attribute) + '\' in the configuration file for \'' + str(tool) + '\''
    text += ', argument \'' + argument + '\' in argument group \'' + str(group) + '\' ' if argument else ' '
    if isType == 'list' or isType == 'dictionary':  text += 'is given a value '
    else: text += 'is given the value \'' + str(value) + '\'. This value is '
    text += 'of an incorrect type (' + isType + '); the expected type is \'' + needsType + '\'. Please correct ' + \
    'this value in the configuration file.'
    self.text.append(text)
    self.writeFormattedText()
    self.terminate()

  # The 'inputs' or 'outputs' argument groups are missing.
  def missingRequiredArgumentGroup(self, tool, isInputs):
    self.text.append('Missing argument group in tool configuration file.')
    self.text.append('The arguments available for the tool \'' + tool + '\' are defined in the \'arguments\' section of the tool ' + \
    'configuration file. Each argument definition is stored in one of the named argument groups. The groups \'inputs\' and ' + \
    '\'outputs\' must be present, but one of them is missing. Please ensure that the configuration file conforms ' + \
    'to all requirements.')
    self.writeFormattedText()
    self.terminate()

  # A tool argument is not accompanied by a dictionary of attributes.
  def toolArgumentHasNoDictionary(self, tool):
    self.text.append('Invalid tool argument definition in configuration file.')
    self.text.append('The tool configuration file has definitions for allowed arguments in a list. This must be a list of dictionaries, each ' + \
    'dictionary containing a variety of required and optional attributes for the tool argument. The configuration file for tool \'' + tool + \
    '\' contains a field in the \'arguments\' section that is not a dictionary. Please modify the configuration file to be compliant with ' + \
    'the configuration standards. The documentation includes information on the format and all allowed attributes for each tool argument.')
    self.writeFormattedText()
    self.terminate()

  # If a tool argument does not have a long form argument.
  def noLongFormForToolArgument(self, tool, group):
    self.text.append('Missing long form for tool argument in configuration file')
    self.text.append('All arguments defined in a tool configuration file, must have a long and short form defined. The long form version is' + \
    'used to identify the argument in all of the relevant data structures as well as for identifying problematic arguments in error messages. ' + \
    'The \'long form argument\' field is missing for one of the arguments in the \'' + group + '\' argument group. Please check the ' + \
    'configuration file for tool \'' + tool + '\' and ensure that the \'long form argument\' attribute is present for all arguments.')
    self.writeFormattedText()
    self.terminate()

  # An argument attribute in the configuration file is invalid.
  def invalidArgumentAttributeInToolConfigurationFile(self, tool, group, argument, attribute, allowedAttributes):
    self.text.append('Invalid argument attribute in tool configuration file: ' + attribute)
    self.text.append('The configuration file for tool \'' + tool + '\' contains the argument \'' + argument + '\' in argument group \'' + group + \
    '\'. This argument contains the attribute \'' + attribute + '\', which is not recognised. The argument attributes allowed in a tool ' + \
    'configuration file are:')
    self.text.append('\t')

    # Create a sorted list of the allowed attributes.
    allowed = []
    for attribute in allowedAttributes: allowed.append(attribute)

    # Add the attributes to the text to be written along with the expected type.
    for attribute in sorted(allowed):
      self.text.append(attribute + ':\t' + str(allowedAttributes[attribute][0]) + ', required = ' + str(allowedAttributes[attribute][1]))

    self.text.append('\t')
    self.text.append('Please remove or correct the invalid attribute in the configuration file.')
    self.writeFormattedText()
    self.terminate()

  # An argument attribute in the configuration file is missing.
  def missingArgumentAttributeInToolConfigurationFile(self, tool, group, argument, attribute, allowedAttributes):
    self.text.append('Missing attribute in tool configuration file for argument: ' + argument)
    self.text.append('The configuration file for tool \'' + tool + '\', argument \'' + argument + '\' in argument group \'' + group + \
    '\' is missing the attribute \'' + attribute + '\'. The following attributes are required for each argument in a tool configuration file:')
    self.text.append('\t')

    # Create a sorted list of the required attributes.
    requiredAttributes = []
    for attribute in allowedAttributes:
       if allowedAttributes[attribute][1]: requiredAttributes.append(attribute)

    # Add the attributes to the text to be written along with the expected type.
    for attribute in sorted(requiredAttributes): self.text.append(attribute + ':\t' + str(allowedAttributes[attribute][0]))

    self.text.append('\t')
    self.text.append('Please add the missing attribute to the tool configuration file.')
    self.writeFormattedText()
    self.terminate()

  # A tool argument is repeated.
  def repeatedToolArgumentInToolConfigurationFile(self, tool, argument, isLongForm):
    self.text.append('Repeated argument in tool configuration file.')
    text = 'long' if isLongForm else 'short'
    self.text.append('Each argument supplied in the tool configuration file must be unique. An argument for tool \'' + tool + \
    '\' has the ' + text + ' form argument \'' + argument + '\', but this has already been ' + \
    'defined for this tool. Please ensure that each argument defined in the configuration file has a unique long and short form argument.')
    self.writeFormattedText()
    self.terminate()

  # An argument is missing from the argument order.
  def missingArgumentInArgumentOrder(self, tool, argument):
    self.text.append('Missing argument in argument order: ' + argument)
    self.text.append('The argument order list in the tool configuration file must contain all of the arguments available to the tool. ' + \
    'The argument \'' + argument + '\' for tool \'' + tool + '\' is not present in the argument order. Please ensure ' + \
    'that the argument order contains all of the arguments for the tool.')
    self.writeFormattedText()
    self.terminate()

  # An invalid argument appears in the argument order.
  def invalidArgumentInArgumentOrder(self, tool, argument):
    self.text.append('Invalid argument in argument order: ' + argument)
    self.text.append('The argument order list must contain only arguments that are available to the tool. The configuration file for tool \'' + \
    tool + '\' contains the argument \'' + argument + '\' which does not correspond to any argument for the tool. Please check and repair ' + \
    'the tool configuration file.')
    self.writeFormattedText()
    self.terminate()

  # An argument is repeated in the argument order.
  def repeatedArgumentInArgumentOrder(self, tool, argument):
    self.text.append('Repeated argument in argument order: ' + argument)
    self.text.append('The argument order list must contain only arguments that are available to the tool and each argument only once. ' + \
    'The configuration file for tool \'' + tool + '\' contains the repeated argument \'' + argument + '\'. Please ensure that each ' + \
    'argument appears only once in the list.')
    self.writeFormattedText()
    self.terminate()

  # An argument is listed as a filename stub, but has no supplied extensions.
  def filenameStubWithNoExtensions(self, tool, argument):
    self.text.append('Argument defined as filename stub with no filename extensions provided.')
    self.text.append('The tool \'' + tool + '\' has an argument \'' + argument + '\' defined as a filename stub. All ' + \
    'filename stub arguments must also provide a list of extensions that are associated with this stub, but this argument does not. ' + \
    'Please check and fix the argument in the configuration file.')
    self.writeFormattedText()
    self.terminate()

  # The 'modify text' list in the 'construct filename' contains a field that has more than one instruction.
  def multipleInstructionsInModifyTextDictionary(self, tool, argument):
    self.text.append('Multiple instructions in construct filename.')
    self.text.append('The tool \'' + tool + '\' has an argument \'' + argument + '\' that has instructions on how to construct the filename. ' + \
    'As part of the instructions, the field \'modify text\' is included. All fields in this list must be dictionaries containing a single ' + \
    'instruction. The filename construction proceeds in the order that the dictionaries appear in the \'modify text\' list and to preserve ' + \
    'the order, each included dictionary can only contain one instruction.')
    self.writeFormattedText()
    self.terminate()

  # The 'modify text' list in the 'construct filename' contains a field that is not a dictionary.
  def notDictionaryInModifyText(self, tool, argument):
    self.text.append('Invalid field in construct filename.')
    self.text.append('The tool \'' + tool + '\' has an argument \'' + argument + '\' that has instructions on how to construct the filename. ' + \
    'As part of the instructions, the field \'modify text\' is included. All fields in this list must be dictionaries, but there is at least ' + \
    'one none dictionary field. Please modify the configuration file to conform to the requirements outlined in the documentation.')
    self.writeFormattedText()
    self.terminate()

  # If the argument is to be modified using instructions from the 'modify argument values' field, 
  # the command field must be present.
  def noCommandInModifyValues(self, tool, argument):
    self.text.append('Missing field in \'modify argument values\'.')
    self.text.append('The tool \'' + tool + '\' has an argument \'' + argument + '\' that has instructions on how to modify the argument ' + \
    'values on the command line. As part of these instructions, the field \'command\' is required, but is missing. This field indicates ' + \
    'how the value should be written to the command line. Please modify the configuration file to conform to the requirements outlined in ' + \
    'the documentation.')
    self.writeFormattedText()
    self.terminate()

  # If the argument is to be modified using instructions from the 'modify argument values' field
  # and the modification is only to occur for specific extensions, terminate if any of the supplied
  # extensions are not valid for this tool argument.
  def invalidExtensionInModifyValues(self, tool, argument, extension, allowedExtensions):
    self.text.append('Invalid extension in \'modify argument values\'.')
    self.text.append('The tool \'' + tool + '\' has an argument \'' + argument + '\' that has instructions on how to modify the argument ' + \
    'values on the command line. These instructions are only executed for files with specified extensions. The extension \'' + extension + \
    '\' is included in the list of extensions to trigger this clause, but this extensions is not a valid extension for the tool argument. ' + \
    'The following extensions are allowed:')
    self.text.append('\t')
    for allowedExtension in allowedExtensions: self.text.append('\t' + allowedExtension)
    self.text.append('\t')
    self.text.append('Please modify the configuration file to conform to the requirements outlined in the documentation.')
    self.writeFormattedText()
    self.terminate()

  # If a files path is defined by another argument and that argument is invalid.
  def invalidArgumentInPathArgument(self, tool, longFormArgument, pathArgument):
    self.text.append('Error with configuration file.')
    self.text.append('The tool \'' + tool + '\' has an argument \'' + longFormArgument + '\' which includes the field \'include path from ' + \
    'argument\'. This field defines another argument for this tool that will be used for the path of the file/directory. The provided ' + \
    'argument \'' + pathArgument + '\' is not a valid argument for this tool. Please update the configuration file to include a valid ' + \
    'argument.')
    self.writeFormattedText()
    self.terminate()

  # If an attribute in the 'argument list' field is not recognised.
  def invalidAttributeInList(self, tool, longFormArgument, attribute, allowedAttributes):
    self.text.append('Invalid attribute in argument list.')
    self.text.append('The configuration file for tool \'' + tool + '\' contains information for the argument \'' + longFormArgument + '\'. This ' + \
    'argument contains the \'argument list\' field, which is a dictionary containing instructions on how to use the values included in the list. ' + \
    'Within this dictionary is the attribute \'' + attribute + '\' which is not recognised. The allowed attributes are:')
    self.text.append('\t')

    # Create a sorted list of the allowed attributes.
    allowed = []
    for attribute in allowedAttributes: allowed.append(attribute)

    # Add the attributes to the text to be written along with the expected type.
    for attribute in sorted(allowed):
      self.text.append(attribute + ':\t' + str(allowedAttributes[attribute][0]) + ', required = ' + str(allowedAttributes[attribute][1]))

    self.text.append('\t')
    self.text.append('Please remove or correct the invalid attribute in the configuration file.')
    self.writeFormattedText()
    self.terminate()

  # If an attribute in the 'argument list' field is not recognised.
  def invalidArgumentInList(self, tool, longFormArgument, argument):
    self.text.append('Invalid argument in argument list.')
    self.text.append('The configuration file for tool \'' + tool + '\' contains information for the argument \'' + longFormArgument + '\'. This ' + \
    'argument contains the \'argument list\' field, which is a dictionary containing instructions on how to use the values included in the list. ' + \
    'Within this dictionary is the attribute \'use argument\' which defines the tool argument to use for the values supplied in the list. ' + \
    'The supplied argument \'' + argument + '\' is not a valid argument for this tool. Please correct the invalid argument in the configuration file.')
    self.writeFormattedText()
    self.terminate()

  # If the 'argument list' mode is invalid.
  def invalidModeInList(self, tool, longFormArgument, mode, allowedModes):
    self.text.append('Invalid mode in argument list.')
    self.text.append('The configuration file for tool \'' + tool + '\' contains information for the argument \'' + longFormArgument + '\'. This ' + \
    'argument contains the \'argument list\' field, which is a dictionary containing instructions on how to use the values included in the list. ' + \
    'Within this dictionary is the attribute \'mode\' which describes how the makefiles should be constructed using the values in the supplied ' + \
    'list. The supplied mode \'' + mode + '\' is not recognised. The allowed modes are:')
    self.text.append('\t')

    # Create a sorted list of the allowed attributes.
    allowed = []
    for mode in allowedModes: allowed.append(mode)

    # Add the attributes to the text to be written along with the expected type.
    for mode in sorted(allowed): self.text.append(mode)

    self.text.append('\t')
    self.text.append('Please remove or correct the invalid attribute in the configuration file.')
    self.writeFormattedText()
    self.terminate()

  # If the instructions for replacing an argument in the event of a stream are invalid.
  def invalidValueArgumentStream(self, tool, longFormArgument, value, allowedValues, isInput):
    field = 'if input is stream' if isInput else 'if output to stream' 
    io    = 'input' if isInput else 'output'
    self.text.append('Invalid instructions for stream.')
    self.text.append('The configuration file for tool \'' + tool + '\' contains information for the argument \'' + longFormArgument + '\'. This ' + \
    'argument contains the \'' + field + '\' field, which describes how to modify the argument in the event that the ' + io + ' is a ' + \
    'stream. The value accompanying this field \'' + value + '\' is not recognised. The allowed values for this field to take are:')
    self.text.append('\t')

    # Create a sorted list of the allowed attributes.
    allowed = []
    for value in allowedValues: allowed.append(value)

    # Add the attributes to the text to be written along with the expected type.
    for value in sorted(allowed): self.text.append(value)

    self.text.append('\t')
    self.text.append('Please remove or correct the invalid attribute in the configuration file.')
    self.writeFormattedText()
    self.terminate()

  # If the input/output argument has instructions on how to proceed with a stream and the value 'replace' is
  # given to the 'if input is stream' or the 'if output to stream', instructions on what to replace the
  # argument/value with are required. If these are missins, terminate.
  def noReplace(self, tool, longFormArgument, isInput):
    field = 'if input is stream' if isInput else 'if output to stream' 
    io    = 'input' if isInput else 'output'
    self.text.append('Invalid instructions for stream.')
    self.text.append('The configuration file for tool \'' + tool + '\' contains information for the argument \'' + longFormArgument + '\'. This ' + \
    'argument contains the \'' + field + '\' field, which describes how to modify the argument in the event that the ' + io + ' is a ' + \
    'stream. The value accompanying this field is \'replace\', which means that instructions on what to replace the argument/value with are ' + \
    'required. These instructions are missing in the configuration file. Please ensure that the \'replace argument with\' dictionary is included ' + \
    'with the argument. This dictionary needs to contain the \'argument\' and \'value\' fields.')
    self.writeFormattedText()
    self.terminate()

  # If the 'replace argument with' dictionary is missing either the 'argument' or 'value' fields.
  def missingAttributeInReplace(self, tool, longFormArgument, attribute, isInput):
    field = 'if input is stream' if isInput else 'if output to stream' 
    io    = 'input' if isInput else 'output'
    self.text.append('Invalid instructions for stream.')
    self.text.append('The configuration file for tool \'' + tool + '\' contains information for the argument \'' + longFormArgument + '\'. This ' + \
    'argument contains the \'' + field + '\' field, which describes how to modify the argument in the event that the ' + io + ' is a ' + \
    'stream. The value accompanying this field is \'replace\', which means that instructions on what to replace the argument/value with are ' + \
    'required. The \'replace argument with\' dictionary provides these instructions, but the contained \'' + attribute + '\' attribute is ' + \
    'missing. Please repair the configuration file.')
    self.writeFormattedText()
    self.terminate()

  # Given a value, return a string representation of the data type.
  def findType(self, providedType):
    if providedType == str: return 'string'
    elif providedType == int: return 'integer'
    elif providedType == float: return 'float'
    elif providedType == bool: return 'Boolean'
    elif providedType == dict: return 'dictionary'
    elif providedType == list: return 'list'
    else: return None

  ##################################
  # Errors with evaluate commands. #
  ##################################

  # The evaluate commands field contains an invalid attribute.
  def invalidAttributeInEvaluate(self, tool, longFormArgument, attribute, allowedAttributes):
    self.text.append('Invalid attribute in \'evaluate command\'.')
    self.text.append('The configuration file for tool \'' + tool + '\' contains information for the argument \'' + longFormArgument + '\'. This ' + \
    'argument contains the \'evaluate command\' field, which contains information on a command that is to be used instead of a value. The ' + \
    'invalid attribute \'' + attribute + '\' appears in this section. Please ensure that this section only contains the attributes listed below:')
    self.text.append('\t')
    for allowedAttribute in allowedAttributes: self.text.append('\t' + allowedAttribute)
    self.writeFormattedText()
    self.terminate()

  # If the attribute type is invalid.
  def invalidTypeEvaluate(self, tool, longFormArgument, attribute, expectedType):
    stringType = self.findType(expectedType)
    self.text.append('Invalid attribute type in \'evaluate command\'.')
    self.text.append('The configuration file for tool \'' + tool + '\' contains information for the argument \'' + longFormArgument + '\'. This ' + \
    'argument contains the \'evaluate command\' field, which contains information on a command that is to be used instead of a value. The ' + \
    'attribute \'' + attribute + '\' is of the wrong type (the expected type is \'' + stringType + '\'). Please ensure that all attributes ' + \
    'are valid.')
    self.writeFormattedText()
    self.terminate()

  # If an attribute is missing.
  def missingAttributeInEvaluate(self, tool, longFormArgument, attribute):
    self.text.append('Missing attribute in \'evaluate command\'.')
    self.text.append('The configuration file for tool \'' + tool + '\' contains information for the argument \'' + longFormArgument + '\'. This ' + \
    'argument contains the \'evaluate command\' field, which contains information on a command that is to be used instead of a value. The ' + \
    'required attribute \'' + attribute + '\' is missing. Please ensure that all required attributes are present.')
    self.writeFormattedText()
    self.terminate()

  # If a required attribute in the 'add values' section is missing.
  def errorInEvaluateValues(self, tool, longFormArgument, attribute):
    self.text.append('Missing attribute in \'evaluate command\', \'add values\' section.')
    self.text.append('The configuration file for tool \'' + tool + '\' contains information for the argument \'' + longFormArgument + '\'. This ' + \
    'argument contains the \'evaluate command\' field, which contains information on a command that is to be used instead of a value. The ' + \
    '\'add values\' list is present, which includes both the \'ID\' and \'argument\' attributes. The \'ID\' is a string present in the ' + \
    'command, and the \'argument\' is the argument whose value should be used in place of this ID. The \'' + attribute + '\' attribute is ' + \
    'missing in at least one of the dictionaries in the \'add values\' section.')
    self.writeFormattedText()
    self.terminate()

  # One of the IDs in the 'add values' section is not present in the command.
  def invalidIDInEvaluate(self, tool, longFormArgument, ID):
    self.text.append('Invalid ID in the \'evaluate command\', \'add values\' section.')
    self.text.append('The configuration file for tool \'' + tool + '\' contains information for the argument \'' + longFormArgument + '\'. This ' + \
    'argument contains the \'evaluate command\' field, which contains information on a command that is to be used instead of a value. The ' + \
    '\'add values\' list is present, which includes both the \'ID\' and \'argument\' attributes. The \'ID\' is a string present in the ' + \
    'command, and the \'argument\' is the argument whose value should be used in place of this ID. The ID \'' + ID + '\' is not present in the ' + \
    'command.')
    self.writeFormattedText()
    self.terminate()

  # An ID in the command has multiple appearances in the 'add values' section.
  def IDDefinedMultipleTimesInEvaluate(self, tool, longFormArgument, ID):
    self.text.append('ID defined multiple times in the \'evaluate command\', \'add values\' section.')
    self.text.append('The configuration file for tool \'' + tool + '\' contains information for the argument \'' + longFormArgument + '\'. This ' + \
    'argument contains the \'evaluate command\' field, which contains information on a command that is to be used instead of a value. The ' + \
    '\'add values\' list is present, which includes both the \'ID\' and \'argument\' attributes. The \'ID\' is a string present in the ' + \
    'command, and the \'argument\' is the argument whose value should be used in place of this ID. Each ID in the command should appear once ' + \
    'and only once in the \'add values\' section, but the ID \'' + ID + '\' is define multiple times.')
    self.writeFormattedText()
    self.terminate()

  # Invalid argument in the 'add values' section.
  def invalidArgumentInEvaluate(self, tool, longFormArgument, ID, argument):
    self.text.append('Invalid argument in the \'evaluate command\', \'add values\' section.')
    self.text.append('The configuration file for tool \'' + tool + '\' contains information for the argument \'' + longFormArgument + '\'. This ' + \
    'argument contains the \'evaluate command\' field, which contains information on a command that is to be used instead of a value. The ' + \
    '\'add values\' list is present, which includes both the \'ID\' and \'argument\' attributes. The \'ID\' is a string present in the ' + \
    'command, and the \'argument\' is the argument whose value should be used in place of this ID. The argument must be a valid argument for ' + \
    'this tool, or take the value \'tool\' which will replace the ID with the name of the tool. The argument \'' + argument + '\' given for the ' + \
    'ID \'' + ID + '\' is not a valid argument.')
    self.writeFormattedText()
    self.terminate()

  ###################################################
  # Errors with filename construction instructions. #
  ###################################################

  # If no construction method is supplied.
  def noConstructionMethod(self, tool, argument, allowedMethods):
    self.text.append('Error with filename construction instructions.')
    self.text.append('Argument \'' + argument + '\' associated with tool \'' + tool + '\' has instructions on how the filename should be ' + \
    'constructed, however, the construction method is not defined. The field \'method\' must be present with one of the following values:')
    self.text.append('\t')
    for method in allowedMethods: self.text.append(method)
    self.text.append('\t')
    self.text.append('Please amend the \'' + tool + '\' configuration file to be consistent with this requirement.')
    self.writeFormattedText()
    self.terminate()

  # If the construction method is unknown.
  def unknownConstructionMethod(self, tool, argument, method, allowedMethods):
    self.text.append('Error with filename construction instructions.')
    self.text.append('Argument \'' + argument + '\' associated with tool \'' + tool + '\' has instructions on how the filename should be ' + \
    'constructed, however, the provided construction method \'' + method + '\' is not recognised. The field \'method\' must be present with ' + \
    'one of the following values:')
    self.text.append('\t')
    for method in allowedMethods: self.text.append(method)
    self.text.append('\t')
    self.text.append('Please amend the \'' + tool + '\' configuration file to be consistent with this requirement.')
    self.writeFormattedText()
    self.terminate()

  # If an unknown attribute appears in the construction instructions.
  def invalidAttributeInConstruction(self, tool, argument, attribute, method, allowedAttributes):
    self.text.append('Error with filename construction instructions.')
    self.text.append('Argument \'' + argument + '\' associated with tool \'' + tool + '\' uses the method \'' + method + '\' to generate the ' + \
    'filename. The attribute \'' + attribute + '\' is included in the instructions, but this is not a valid attribute for this method of ' + \
    'filename construction. The allowed attributes for this method are:')
    self.text.append('\t')
    for allowedAttribute in allowedAttributes:
      dataType   = allowedAttributes[allowedAttribute][0]
      isRequired = allowedAttributes[allowedAttribute][1]
      if isRequired: self.text.append(allowedAttribute + ' (' + str(dataType) + '): required.')
      else: self.text.append(allowedAttribute + ' (' + str(dataType) + '): optional.')
    self.text.append('\t')
    self.text.append('Please amend the \'' + tool + '\' configuration file to be consistent with the requirements.')
    self.writeFormattedText()
    self.terminate()

  # The construction instructions included in the modify text field, must be dictionaries, each of which contains
  # a list as the value.
  def nonListInConstruction(self, tool, argument, field):
    self.text.append('Error with filename construction instructions.')
    self.text.append('Argument \'' + argument + '\' associated with tool \'' + tool + '\' has instructions on how the filename should be ' + \
    'constructed. As part of the construction, there are instructions on how to modify the text. This a list of dictionaries, where each ' + \
    'dictionary contains an instruction as the key and a list of strings as the value. The field \'' + field + '\', however does not have ' + \
    'a list as the value. Please check the construction instructions.')
    self.writeFormattedText()
    self.terminate()

  # The construction instructions included in the modify text field, must be dictionaries, each of which contains
  # a list as the value. Ths list needs to be strings, no dicts or lists.
  def invalidStringInConstruction(self, tool, argument, field):
    self.text.append('Error with filename construction instructions.')
    self.text.append('Argument \'' + argument + '\' associated with tool \'' + tool + '\' has instructions on how the filename should be ' + \
    'constructed. As part of the construction, there are instructions on how to modify the text. This a list of dictionaries, where each ' + \
    'dictionary contains an instruction as the key and a list of strings as the value. The field \'' + field + '\', however contains a value ' + \
    'that is either a dictionary or a list. Please check the construction instructions.')
    self.writeFormattedText()
    self.terminate()

  # The type is incorrect.
  def incorrectTypeInConstruction(self, tool, argument, attribute, method, value, expectedType):

    # Find the given type.
    isType    = self.findType(type(value))
    needsType = self.findType(expectedType)
    if isType == None: isType = 'Unknown'
    if needsType == None: needsType = 'Unknown'

    self.text.append('Invalid attribute value in filename construction instructions.')
    text = 'Argument \'' + argument + '\' associated with tool \'' + tool + '\' uses the method \'' + method + '\' to generate the ' + \
    'filename. The attribute \'' + attribute + '\' '
    if isType == 'list' or isType == 'dictionary':  text += 'is given a value '
    else: text += 'is given the value \'' + str(value) + '\'. This value is '
    text += 'of an incorrect type (' + isType + '); the expected type is \'' + needsType + '\'. Please correct ' + \
    'this value in the configuration file.'
    self.text.append(text)
    self.writeFormattedText()
    self.terminate()

  # A required values is missing.
  def missingAttributeInConstruction(self, tool, argument, attribute, method, allowedAttributes):
    self.text.append('Missing attribute in construction instructions.')
    self.text.append('Argument \'' + argument + '\' associated with tool \'' + tool + '\' uses the method \'' + method + '\' to generate the ' + \
    'filename. The attribute \'' + attribute + '\' is required in the \'construct filename\' section of the configuration file for this ' + \
    'argument, but it is missing. The following attributes are required in the \'construct filename\' section, if the \'' + method + \
    '\' method is being used:')
    self.text.append('\t')

    # Create a sorted list of the required attributes.
    requiredAttributes = []
    for attribute in allowedAttributes:
       if allowedAttributes[attribute][1]: requiredAttributes.append(attribute)

    # Add the attributes to the text to be written along with the expected type.
    for attribute in sorted(requiredAttributes): self.text.append(attribute + ':\t' + str(allowedAttributes[attribute][0]))

    self.text.append('\t')
    self.text.append('Please add the missing attribute to the configuration file.')
    self.writeFormattedText()
    self.terminate()

  # An argument used in the 'add additional values' section is invalid.
  def invalidArgumentInConstruction(self, tool, argument, addArgument, allowedArguments, section):
    self.text.append('Invalid argument in construction instructions.')
    self.text.append('Argument \'' + argument + '\' associated with tool \'' + tool + '\' has instructions on how to construct the filename in ' + \
    'the absence of a defined value. As part of this construction, the value from another tool argument (' + addArgument + ') is used as ' + \
    'instructed in the \'' + section + '\' section. This argument is not a valid argument for this tool. Arguments appearing in this section ' + \
    'must be one of the following:')
    self.text.append('\t')
    for allowedArgument in allowedArguments: self.text.append(allowedArgument)
    self.text.append('\t')
    self.text.append('Please correct the configuration file.')
    self.writeFormattedText()
    self.terminate()

  ##########################################################
  # Errors associated with trying to get tool information. #
  ##########################################################

  # Attempt to get information on a general tool attribute when the requested tool does not exist.
  def invalidToolInGeneralToolAttributes(self, tool, attribute):
    self.text.append('Attempt to get attributes for a non-existent tool: ' + tool)
    self.text.append('The configurationClass method \'getGeneralAttribute\' was called to get the attribute \'' + attribute + \
    '\' for tool \'' + tool + '\', however, the requested tool does not exist.')
    self.writeFormattedText()
    self.terminate()

  # Attempt to get information on a tool argument when the requested tool does not exist.
  def invalidToolInToolArgumentAttributes(self, tool, argument, attribute, problemID):
    self.text.append('Attempt to get argument attributes for a non-existent tool: ' + tool)
    self.text.append('The configurationClass method \'getArgumentAttribute\' was called to get the attribute \'' + attribute + \
    '\' for argument \'' + argument + '\' for tool \'' + tool + '\'.')
    self.text.append('\t')
    if problemID == 'tool': self.text.append('The requested tool does not exist.')
    elif problemID == 'argument': self.text.append('The requested argument does not exist.')
    else: self.text.append('An unknown problem with this request has occurred.')
    self.writeFormattedText()
    self.terminate()

  # Attempt to get a specific argument for an invalid tool.
  def invalidToolInGetArguments(self, tool):
    self.text.append('Attempt to get all arguments for a non-existent tool: ' + tool)
    self.text.append('The configurationClass method \'getArguments\' was called to get the all of the arguments for the tool \'' + tool + \
    '\'. The requested tool does not exist.')
    self.writeFormattedText()
    self.terminate()

  # Requested tool is not present in the dictionaries of long and short form arguments.
  def missingToolInGetLongFormArgument(self, tool):
    self.text.append('Unknown tool: ' + tool)
    self.text.append('The configurationClass method \'getLongFormArgument\' was called to find the long form of an argument for the ' + \
    'tool \'' + tool + '\'. This is an unknown tool and so the argument cannot be processed.')
    self.writeFormattedText()
    self.terminate()

  # An unknown command line argument was requested,
  def unknownToolArgument(self, tool, argument):
    self.text.append('Unknown argument: ' + str(argument))
    text = 'The argument \'' + str(argument) + '\' was included on the command line, but is not a valid argument for the current tool (' + \
    tool + ').'
    self.text.append(text)
    self.writeFormattedText()
    self.terminate()

  ##############################################
  # Errors with a pipeline configuration file. #
  ##############################################

  # Attribute has the wrong type.
  def incorrectTypeInPipelineConfigurationFile(self, pipeline, attribute, value, expectedType, section):

    # Find the given type.
    isType    = self.findType(type(value))
    needsType = self.findType(expectedType)
    if isType == None: isType = 'Unknown'
    if needsType == None: needsType = 'Unknown'
    self.text.append('Invalid attribute value in pipeline configuration file.')
    text = 'The attribute \'' + str(attribute) + '\' in the \'' + section + '\' section of the configuration file for pipeline \'' + \
    str(pipeline) + '\' '
    if isType == 'list' or isType == 'dictionary':  text += 'is given a value '
    else: text += 'is given the value \'' + str(value) + '\'. This value is '
    text += 'of an incorrect type (' + isType + '); the expected type is \'' + needsType + '\'. Please correct ' + \
    'this value in the configuration file.'
    self.text.append(text)
    self.writeFormattedText()
    self.terminate()

  # A task in the tasks section is not accompanied by a dictionary.
  def taskIsNotDictionary(self, pipeline, task):
    self.text.append('Task not supplied with dictionary in \'tasks\' section of pipeline configuration file.')
    self.text.append('The pipeline configuration file contains a section \'tasks\' which assigns certain parameters to each task in the ' + \
    'pipeline. Each listed task must be accompanied by a dictionary with key, value pairs describing these parameters. The configuration ' + \
    'file for pipeline \'' + pipeline + '\' contains the task \'' + task + '\' which is not accompanied by a dictionary. Please check and ' + \
    'correct the configuration file.')
    self.writeFormattedText()
    self.terminate()

  # An general entry in the configuration file is invalid.
  def invalidAttributeInTasks(self, pipeline, task, attribute, allowedAttributes):
    self.text.append('Invalid attribute in tasks section of a pipeline configuration file: ' + attribute)
    self.text.append('In the tasks section of the configuration file for pipeline \'' + pipeline + '\', there is a problem with the contained ' + \
    'information for task \'' + task + '\'. The configuration file contains the attribute \'' + attribute + '\', which is an unrecognised ' + \
    'attribute which is not permitted. The attributes allowed for each task in the tasks section of the pipeline configuration file are:')
    self.text.append('\t')

    # Create a sorted list of the allowed attributes.
    allowed = []
    for attribute in allowedAttributes: allowed.append(attribute)

    # Add the attributes to the text to be written along with the expected type.
    for attribute in sorted(allowed):
      self.text.append(attribute + ':\t' + str(allowedAttributes[attribute][0]) + ', required = ' + str(allowedAttributes[attribute][1]))

    self.text.append('\t')
    self.text.append('Please remove or correct the invalid attribute in the configuration file.')
    self.writeFormattedText()
    self.terminate()

  # A pipeline attribute is missing.
  def missingAttributeInPipelineConfigurationFile(self, pipeline, attribute, allowedAttributes, section, ID):
    self.text.append('Missing attribute in ' + section + ' section of configuration file: ' + pipeline)
    text = 'The pipeline configuration file for \'' + pipeline + '\' is missing the \'' + section + '\' attribute \'' + attribute + '\''
    if ID: text += ' from node ID \'' + ID + '\'. '
    else: text += '. '
    text += 'The following attributes are required in the \'' + section + '\' section of the pipeline configuration file:'
    self.text.append(text)
    self.text.append('\t')

    # Create a sorted list of the required attributes.
    requiredAttributes = []
    for attribute in allowedAttributes:
       if allowedAttributes[attribute][1]: requiredAttributes.append(attribute)

    # Add the attributes to the text to be written along with the expected type.
    for attribute in sorted(requiredAttributes): self.text.append(attribute + ':\t' + str(allowedAttributes[attribute][0]))

    self.text.append('\t')
    self.text.append('Please add the missing attribute to the configuration file.')
    self.writeFormattedText()
    self.terminate()

  # If the tool supplied for a task is invalid.
  def invalidToolInPipelineConfigurationFile(self, pipeline, task, tool):
    self.text.append('Invalid tool defined for task: ' + task)
    self.text.append('In the \'tasks\' section of the configuration file for pipeline \'' + pipeline + '\', the task \'' + task + \
    '\' is defined as using the tool \'' + tool + '\'. This tool does not have a defined configuration file of its own and so cannot ' + \
    'be used. Please check the configuration file and supply a valid tool.')
    self.writeFormattedText()
    self.terminate()

  # If the pipeline node does not have the 'ID' attributes.
  def noIDInPipelineNode(self, pipeline):
    self.text.append('No ID for a pipeline node.')
    self.text.append('Each node in the \'nodes\' section of the pipeline configuration file contains information about pipeline ' + \
    'arguments, which tasks connect to the same nodes etc. Each of these nodes must contain the \'ID\' attribute, so that the node can ' + \
    'be identified. A node in the configuration file for \'' + pipeline + '\' does not have this attribute. Please check the configuration ' + \
    'file and ensure that each node has a unique \'ID\' attribute.')
    self.writeFormattedText()
    self.terminate()

  # If the attribute is not recognised.
  def invalidAttributeInNodes(self, pipeline, ID, attribute, allowedAttributes):
    self.text.append('Invalid attribute in nodes section of the configuration file for pipeline: ' + pipeline)
    text = 'The \'nodes\' section of the configuration file for pipeline \'' + pipeline + '\' contains the node identified with the ID \'' + \
    ID + '\'. This node contains the attribute \'' + attribute + '\'. This is an unrecognised attribute which is not permitted. The ' + \
    'attributes allowed in the nodes section of the pipeline configuration file are:'
    self.text.append(text)
    self.text.append('\t')

    # Create a sorted list of the allowed attributes.
    allowed = []
    for attribute in allowedAttributes: allowed.append(attribute)

    # Add the attributes to the text to be written along with the expected type.
    for attribute in sorted(allowed):
      self.text.append(attribute + ':\t' + str(allowedAttributes[attribute][0]) + ', required = ' + str(allowedAttributes[attribute][1]))

    self.text.append('\t')
    self.text.append('Please remove or correct the invalid attribute in the configuration file.')
    self.writeFormattedText()
    self.terminate()

  # A node in the nodes section is not a dictionary.
  def nodeIsNotADictionary(self, pipeline):
    self.text.append('Node in \'nodes\' section of the pipeline configuration file is not a dictionary.')
    self.text.append('The pipeline configuration file contains a section \'nodes\' which defines a node associated with, for example, ' + \
    'pipeline arguments, or describes which arguments of different tasks point to the same files. The configuration file for pipeline \'' + 
    pipeline + '\' contains a node which is not a dictionary. Please check and correct the configuration file.')
    self.writeFormattedText()
    self.terminate()

  # A task in a pipeline node definition is invalid.
  def invalidTaskInNode(self, pipeline, nodeID, task, isGreedy):
    self.text.append('Invalid task in pipeline configuration file node: ' + nodeID)
    text = 'The \'nodes\' section of the pipeline configuration file for pipeline \'' + pipeline + '\' contains a node in the \'nodes\' ' + \
    'section with the ID \'' + nodeID + '\'. The contained \''
    if isGreedy: text += 'greedy '
    text += 'tasks\' section contains the task \'' + task + '\' which is not a task available in the pipeline. Please check the configuration ' + \
    'file and ensure that all tasks are valid.'
    self.text.append(text)
    self.writeFormattedText()
    self.terminate()

  # If a task argument appears in multiple nodes in the pipeline configuration file.
  def repeatedArgumentInNode(self, task, argument, nodeIDs):
    self.text.append('Task argument in multiple configuration file nodes.')
    self.text.append('To avoid problems with merging pipeline graph nodes, a task argument is only permitted in a single node in the pipeline ' + \
    'configuration file. The argument \'' + argument + '\' for task \'' + task + '\' appears in the following configuration file nodes:')
    self.text.append('\t')
    for nodeID in nodeIDs: self.text.append(nodeID)
    self.text.append('\t')
    self.text.append('These configuration file nodes should be compressed into a single node. If the argument is for an argument stub, ensure ' + \
    'that the extensions are specified for linked arguments. Alternatively, task arguments can be defined in other configuration nodes as edges. ' + \
    'Please see the documentation for further information on this subject.')
    self.writeFormattedText()
    self.terminate()

  # If the attribute is not recognised in evaluate command block.
  def invalidAttributeInEvaluateCommand(self, pipeline, ID, attribute, allowedAttributes):
    self.text.append('Invalid attribute in nodes section of the configuration file for pipeline: ' + pipeline)
    self.text.append('The \'nodes\' section of the configuration file for pipeline \'' + pipeline + '\' contains the node identified with the ID ' + \
    '\'' + ID + '\'. This node contains the \'evaluate command\' dictionary containing the attribute \'' + attribute + '\'. This is an ' + \
    'unrecognised attribute which is not permitted. The attributes allowed in the evaluate command dictionary are:')
    self.text.append('\t')

    # Create a sorted list of the allowed attributes.
    allowed = []
    for attribute in allowedAttributes: allowed.append(attribute)

    # Add the attributes to the text to be written along with the expected type.
    for attribute in sorted(allowed):
      self.text.append(attribute + ':\t' + str(allowedAttributes[attribute][0]) + ', required = ' + str(allowedAttributes[attribute][1]))

    self.text.append('\t')
    self.text.append('Please remove or correct the invalid attribute in the configuration file.')
    self.writeFormattedText()
    self.terminate()

  # A pipeline attribute is missing.
  def missingAttributeInEvaluateCommand(self, pipeline, ID, attribute, allowedAttributes):
    self.text.append('Missing attribute in nodes section of configuration file: ' + pipeline)
    self.text.append('The \'nodes\' section of the configuration file for pipeline \'' + pipeline + '\' contains the node identified with the ' + \
    'ID \'' + ID + '\'. This node contains the \'evaluate command\' dictionary, but is missing the attribute \'' + attribute + '\'. The ' + \
    'following attributes are required in the \'evaluate commands\' section of the pipeline configuration file:')
    self.text.append('\t')

    # Create a sorted list of the required attributes.
    requiredAttributes = []
    for attribute in allowedAttributes:
       if allowedAttributes[attribute][1]: requiredAttributes.append(attribute)

    # Add the attributes to the text to be written along with the expected type.
    for attribute in sorted(requiredAttributes): self.text.append(attribute + ':\t' + str(allowedAttributes[attribute][0]))

    self.text.append('\t')
    self.text.append('Please add the missing attribute to the configuration file.')
    self.writeFormattedText()
    self.terminate()

  # If the attribute is not recognised in evaluate command values block.
  def invalidAttributeInEvaluateCommandValues(self, pipeline, ID, attribute, allowedAttributes):
    self.text.append('Invalid attribute in nodes section of the configuration file for pipeline: ' + pipeline)
    self.text.append('The \'nodes\' section of the configuration file for pipeline \'' + pipeline + '\' contains the node identified with the ID ' + \
    '\'' + ID + '\'. This node contains the \'evaluate command\' dictionary containing the \'add values\' field. Within this dictionary, the ' + \
    'attribute \'' + attribute + '\' is defined, but this is an unrecognised attribute. The attributes allowed in the evaluate command, ' + \
    ' \'add values\' dictionary are:')
    self.text.append('\t')

    # Create a sorted list of the allowed attributes.
    allowed = []
    for attribute in allowedAttributes: allowed.append(attribute)

    # Add the attributes to the text to be written along with the expected type.
    for attribute in sorted(allowed):
      self.text.append(attribute + ':\t' + str(allowedAttributes[attribute][0]) + ', required = ' + str(allowedAttributes[attribute][1]))

    self.text.append('\t')
    self.text.append('Please remove or correct the invalid attribute in the configuration file.')
    self.writeFormattedText()
    self.terminate()

  # An attribute is missing in the evaluate commands, add values dictionary.
  def missingAttributeInEvaluateCommandValues(self, pipeline, ID, attribute, allowedAttributes):
    self.text.append('Missing attribute in nodes section of configuration file: ' + pipeline)
    self.text.append('The \'nodes\' section of the configuration file for pipeline \'' + pipeline + '\' contains the node identified with the ' + \
    'ID \'' + ID + '\'. This node contains the \'evaluate command\' dictionary, which in turn contains the \'add values\' dictionary. The ' + \
    'attribute \'' + attribute + '\' is required in this dictionary, but is missing. The following attributes are required in the \'add values\'' + \
    ' dictionary within the \'evaluate commands\' section of the pipeline configuration file:')
    self.text.append('\t')

    # Create a sorted list of the required attributes.
    requiredAttributes = []
    for attribute in allowedAttributes:
       if allowedAttributes[attribute][1]: requiredAttributes.append(attribute)

    # Add the attributes to the text to be written along with the expected type.
    for attribute in sorted(requiredAttributes): self.text.append(attribute + ':\t' + str(allowedAttributes[attribute][0]))

    self.text.append('\t')
    self.text.append('Please add the missing attribute to the configuration file.')
    self.writeFormattedText()
    self.terminate()

  # If a task in the evaluate command, add values section is invalid.
  def unknownTaskInEvaluateCommandValues(self, pipeline, ID, task):
    self.text.append('Unknown task in evaluate command section in the configuration file: ' + pipeline)
    self.text.append('The \'nodes\' section of the configuration file for pipeline \'' + pipeline + '\' contains the node identified with the ' + \
    'ID \'' + ID + '\'. This node contains the \'evaluate command\' dictionary, which in turn contains the \'add values\' dictionary. Within ' + \
    'this section, one of the entries points to the task \'' + task + '\', but this is not a task available in the current pipeline. Please ' + \
    'check the configuration file and ensure that the tasks included in the section are valid.')
    self.writeFormattedText()
    self.terminate()

  # If an ID in the evaluate command, add values section is no present in the command.
  def unknownIDInEvaluateCommandValues(self, pipeline, ID, valueID):
    self.text.append('Unknown task in evaluate command section in the configuration file: ' + pipeline)
    self.text.append('The \'nodes\' section of the configuration file for pipeline \'' + pipeline + '\' contains the node identified with the ' + \
    'ID \'' + ID + '\'. This node contains the \'evaluate command\' dictionary, which in turn contains the \'add values\' dictionary. Within ' + \
    'this section, one of the entries has the ID \'' + valueID + '\'. This ID must be present in the string included in the \'command\' ' + \
    'field, but it is not. Please check the configuration file and ensure that the IDs in the \'add values\' section are present in the command.')
    self.writeFormattedText()
    self.terminate()

  # If a task/argument has multiple commands set to evaluate at run time.
  def multipleEvaluationsForArgument(self, pipeline, ID, task, argument):
    self.text.append('Argument given multiple commands to evaluate in the configuration file: ' + pipeline)
    self.text.append('The \'nodes\' section of the configuration file for pipeline \'' + pipeline + '\' contains the node identified with the ' + \
    'ID \'' + ID + '\'. This node contains the \'evaluate command\' dictionary. This gives instructions on how to generate a value if none ' + \
    'has been supplied. The task \'' + task + '\', argument \'' + argument + '\' has already been given a command to evaluate. Only a single ' + \
    'command can be evaluated at run time. Please ensure that the task/argument pair is only listed in a single configuration file node with ' + \
    'instructions on evaluating a command.')
    self.writeFormattedText()
    self.terminate()

  # If an evaluate command ID is repeated.
  def nonUniqueEvaluateCommandID(self, pipeline, ID, observedID):
    self.text.append('Duplicated ID in evaluate command field in the configuration file: ' + pipeline)
    self.text.append('The \'nodes\' section of the configuration file for pipeline \'' + pipeline + '\' contains the node identified with the ' + \
    'ID \'' + ID + '\'. This node contains the \'evaluate command\' dictionary. This gives instructions on how to generate a value if none ' + \
    'has been supplied and includes a command with IDs indicating the task/argument pair to use to include in the command. The ID \'' + observedID + \
    '\' appears multiple times for this command, but the \'add values\' field must be comprised of values defined with unique IDs. Please check ' + \
    'and repair the pipeline configuration file.')
    self.writeFormattedText()
    self.terminate()

  # If an argument in the evaluate commands block is invalid.
  def invalidArgumentInEvaluateCommand(self, ID, task, argument):
    self.text.append('Invalid argument in evaluate command field.')
    self.text.append('The \'nodes\' section of the pipeline configuration file contains a node containing the \'evaluate command\' dictionary. ' + \
    'This gives instructions on how to generate a value if none has been supplied and includes a command with IDs indicating the task/argument ' + \
    'pair to use to include in the command. The ID \'' + ID + '\' uses the task \'' + task + '\', but the supplied argument \'' + argument + \
    '\' is not valid. Please check and repair the pipeline configuration file.')
    self.writeFormattedText()
    self.terminate()

  # If a node is shared by multiple arguments, one of which is set as 'read json file', but
  # none of the arguments for that node output a json file.
  def noJsonOutput(self, nodeID, task):
    self.text.append('Error with pipeline configuration file node.')
    self.text.append('The pipeline configuration node \'' + nodeID + '\' in the \'nodes\' section defines a number of task arguments that ' + \
    'share a node in the pipeline graph. One of these tasks (' + task + ') is set to \'read json file\'. This is ' + \
    'used to indicate that this task will use a json file to set parameters for the tool. For this to be valid, the configuration file node ' + \
    'must also contain a task argument that outputs a json file, but none of the other task arguments in this node do. Please check and repair ' + \
    'the configuration file.')
    self.writeFormattedText()
    self.terminate()

  # If the argument is not a valid argument for the listed task.
  def invalidToolArgument(self, configNodeID, task, tool, argument, validArguments):
    self.text.append('Invalid tool argument in pipeline configuration file.')
    self.text.append('The argument \'' + argument + '\' appears in the pipeline configuration file \'nodes\' section, in the node with the ' + \
    'ID \'' + configNodeID + '\', associated with the task \'' + task + '\'. This task uses the tool \'' + tool + '\' and the argument ' + \
    'is not valid for this tool. Please check the configuration file and amend this invalid argument. The allowed arguments for this tool are:')
    self.text.append('\t')
    for toolArgument in validArguments: self.text.append(toolArgument)
    self.writeFormattedText()
    self.terminate()

  # A non-filename stub argument is associated with a filename stub argument, but the
  # required extension is not set.
  def noExtensionInNode(self, configNodeID, task, argument, stubArguments):
    self.text.append('Missing extension in pipeline configuration file.')
    self.text.append('The node \'' + configNodeID + '\' in the \'nodes\' section of the pipeline configuration file contains the task/' + \
    'argument pair \'' + task + ' ' + argument + '\'. Also contained in the node are the task/argument pairs:')
    self.text.append('\t')
    for stubTask, stubArgument in stubArguments: self.text.append(stubTask + ' ' + stubArgument)
    self.text.append('\t')
    self.text.append('These arguments point to a filename stub. The argument \'' + task + ' ' + argument + '\' is not a filename stub and ' + \
    'so the extension that it expects needs to be specified in the node. Please see the documentation for details on how this is achieved.')
    self.writeFormattedText()
    self.terminate()

  # A pipeline argument is repeated.
  def nonUniquePipelineFormArgument(self, nodeID, longFormArgument, shortFormArgument, longFormError):
    if longFormError: text = 'long form argument \'' + longFormArgument + '\''
    else: text = 'short form argument \'' + shortFormArgument + '\''
    self.text.append('Repeated argument in the pipeline configuration file.')
    self.text.append('The pipeline configuration file contains a node in the \'nodes\' section with the ID \'' + nodeID + '\'. The ' + \
    'arguments associated with this node are \'' + longFormArgument + ' (' + shortFormArgument + ')\', but the ' + text + ' appears multiple ' + \
    'times in the configuration file. Each long and short form argument must be unique. Please remove this duplication from the ' + \
    'configuration file.')
    self.writeFormattedText()
    self.terminate()

  ################################################################
  # Errors associated with parameter sets in configuration file. #
  ################################################################

  # Requested parameter set does not exist.
  def missingParameterSet(self, name, parameterSetName, isPipeline, availableParameterSets):
    runType = 'pipeline' if isPipeline else 'tool'
    self.text.append('Requested parameter set for ' + runType + ' \'' + name + '\' does not exist: ' + parameterSetName)
    self.text.append('The parameter set \'' + parameterSetName + '\' was requested, but no parameter set with this name is available in the ' + runType + \
    ' configuration file or the external parameter sets configuration file for the ' + runType + '. The parameter sets available for this ' + runType + \
    ' are:')
    self.text.append('\t')
    for parameterSet in availableParameterSets: self.text.append(parameterSet)
    self.writeFormattedText()
    self.terminate()

  # ParameterSet has no ID.
  def noIDForParameterSet(self, runName, isPipeline):
    runType = 'pipeline' if isPipeline else 'tool'
    self.text.append('ParameterSet has no ID')
    self.text.append('The parameter sets section of the configuration file for ' + runType + ' configuration file \'' + runName + '\' contains ' + \
    'an parameter set with no ID. All parameter sets must have a unique identifier. Please check the configuration file and ensure that all parameter sets ' + \
    'are correctly formed with a valid ID.')
    self.writeFormattedText()
    self.terminate()

  # Missing attribute.
  def missingAttributeInParameterSet(self, runName, attribute, allowedAttributes, parameterSetID, isPipeline):
    runType = 'pipeline' if isPipeline else 'tool'
    self.text.append('Missing attribute in parameter sets section of ' + runType + ' configuration file: ' + runName)
    self.text.append('The ' + runType + ' configuration file for \'' + runName + '\' is missing the parameter sets attribute \'' + attribute + \
    '\' from parameter set \'' + parameterSetID + '\'. The following attributes are required in the parameter sets section of the pipeline configuration file:')
    self.text.append('\t')

    # Create a sorted list of the required attributes.
    requiredAttributes = []
    for attribute in allowedAttributes:
       if allowedAttributes[attribute][1]: requiredAttributes.append(attribute)

    # Add the attributes to the text to be written along with the expected type.
    for attribute in sorted(requiredAttributes): self.text.append(attribute + ':\t' + str(allowedAttributes[attribute][0]))

    self.text.append('\t')
    self.text.append('Please add the missing attribute to the configuration file.')
    self.writeFormattedText()
    self.terminate()

  # Invalid attribute in parameter set.
  def invalidAttributeInParameterSet(self, pipeline, parameterSet, attribute, allowedAttributes):
    self.text.append('Invalid attribute in the parameter sets section of the configuration file for pipeline: ' + pipeline)
    text = 'The \'parameter sets\' section contains information for the parameter set \'' + parameterSet + '\' in the configuration file for pipeline \'' + \
    pipeline + '\'. This parameter set contains the attribute \'' + attribute + '\'. This is an unrecognised attribute which is not permitted. The ' + \
    'attributes allowed in the parameter sets section of the pipeline configuration file are:'
    self.text.append(text)
    self.text.append('\t')

    # Create a sorted list of the allowed attributes.
    allowed = []
    for attribute in allowedAttributes: allowed.append(attribute)

    # Add the attributes to the text to be written along with the expected type.
    for attribute in sorted(allowed):
      self.text.append(attribute + ':\t' + str(allowedAttributes[attribute][0]) + ', required = ' + str(allowedAttributes[attribute][1]))

    self.text.append('\t')
    self.text.append('Please remove or correct the invalid attribute in the configuration file.')
    self.writeFormattedText()
    self.terminate()

  # Invalid attribute in parameter set node.
  def invalidAttributeInParameterSetNode(self, runName, parameterSet, attribute, allowedAttributes, isPipeline):
    runType = 'pipeline' if isPipeline else 'tool'
    self.text.append('Invalid attribute in a node in the parameter sets section of the configuration file for ' + runType + ': ' + runName)
    self.text.append('The \'parameter sets\' section in the ' + runType + ' configuration file \'' + runName + '\' contains information for ' + \
    'the parameter set \'' + parameterSet + '\'. This parameter set contains the attribute \'' + attribute + '\' in one of the parameter set nodes. This is an ' + \
    'unrecognised attribute which is not permitted. The attributes allowed for each node in the parameter sets section of the ' + runType + \
    ' configuration file are:')
    self.text.append('\t')

    # Create a sorted list of the allowed attributes.
    allowed = []
    for attribute in allowedAttributes: allowed.append(attribute)

    # Add the attributes to the text to be written along with the expected type.
    for attribute in sorted(allowed):
      self.text.append(attribute + ':\t' + str(allowedAttributes[attribute][0]) + ', required = ' + str(allowedAttributes[attribute][1]))

    self.text.append('\t')
    self.text.append('Please remove or correct the invalid attribute in the configuration file.')
    self.writeFormattedText()
    self.terminate()

  # Missing attribute in a node.
  def missingAttributeInPipelineParameterSetNode(self, runName, parameterSet, attribute, allowedAttributes, isPipeline):
    runType = 'pipeline' if isPipeline else 'tool'
    self.text.append('Missing attribute in parameter sets section of ' + runType + ' configuration file: ' + runName)
    self.text.append('The ' + runType + ' configuration file for \'' + runName + '\' contains a section \'parameter sets\' for defining preset ' + \
    'values for the ' + runType + '. One of the nodes in the parameter set \'' + parameterSet + '\' is missing the attribute \'' + attribute + '\'. ' + \
    'The following attributes are required for each node in the \'parameter sets\' section of the ' + runType + ' configuration file:')
    self.text.append('\t')

    # Create a sorted list of the required attributes.
    requiredAttributes = []
    for attribute in allowedAttributes:
       if allowedAttributes[attribute][1]: requiredAttributes.append(attribute)

    # Add the attributes to the text to be written along with the expected type.
    for attribute in sorted(requiredAttributes): self.text.append(attribute + ':\t' + str(allowedAttributes[attribute][0]))

    self.text.append('\t')
    self.text.append('Please add the missing attribute to the configuration file.')
    self.writeFormattedText()
    self.terminate()

  # Duplicated parameter set.
  def duplicateParameterSet(self, runName, parameterSetID, isPipeline):
    runType = 'pipeline' if isPipeline else 'tool'
    self.text.append('Duplicated parameter set information for ' + runType + ': ' + runName)
    self.text.append('Each parameter set appearing in the parameter sets section of the configuration file must be unique. This includes any ' + \
    'parameter sets included in the external parameter set file \'' + runName + '_parameter sets\'. The parameter set \'' + parameterSetID + '\' appears multiple ' + \
    'times for this ' + runType + '. Please ensure that all parameter set names are unique.')
    self.writeFormattedText()
    self.terminate()

  # Duplicate attribute in parameter set nodes.
  def duplicateNodeInParameterSet(self, runName, parameterSetID, attributeType, attribute, isPipeline):
    runType = 'pipeline' if isPipeline else 'tool'
    self.text.append('Duplicated parameter set attribute for ' + runType + ': ' + runName)
    self.text.append('Each parameter set contains a set of nodes defining the values to be given to certain arguments. Within any one parameter set, ' + \
    'each ID and argument must be unique. The ' + attributeType + ' \'' + attribute + '\' appears multiple times in the ' + runType + \
    ' parameter set \'' + parameterSetID + '\'. Please ensure that each node for each parameter set in the configuration file contains a unique ID and ' + \
    'argument.')
    self.writeFormattedText()
    self.terminate()

  # Invalid argument in parameter set.
  def invalidArgumentInToolParameterSet(self, runName, parameterSetName, argumentID, argument):
    self.text.append('Invalid argument in parameter set: ' + parameterSetName)
    self.text.append('The configuration file contains the parameter set \'' + parameterSetName + '\', which  contains information for the argument \'' + \
    argument + '\' (ID: \'' + argumentID + '\'). This argument is not a valid argument for the tool. Please ensure that all arguments defined in ' + \
    'the parameter sets section are valid for the tool.')
    self.writeFormattedText()
    self.terminate()

  # Invalid argument in 
  def invalidArgumentInParameterSet(self, runName, parameterSet, ID, originalArgument, isPipeline):
    runType = 'tool' if not isPipeline else 'pipeline'
    self.text.append('Invalid argument in parameter set.')
    self.text.append('The configuration file for the ' + runType + ' \'' + runName + '\' contains information for a parameter set with ' + \
    'the name \'' + parameterSet + '\'. This parameter set contains a node with ID \'' + ID + '\' that defines values for the argument \'' + \
    originalArgument + '\'. This is not a valid argument for this ' + runType + ', however. Please check the configuration file and ensure ' + \
    'that all supplied parameter set arguments are valid.')
    self.writeFormattedText()
    self.terminate()

  ###################################################
  # Errors associated with getting node attributes. *
  ###################################################

  # If a a node attribute was requested, but the node does not exist in the graph, terminate.
  def missingNodeInAttributeRequest(self, node):
    self.text.append('Unknown pipeline graph node attribute requested.')
    text = 'A pipeline node attribute was requested (using function getGraphNodeAttribute), however, the requested node \'' + str(node) + \
    '\' does not exist in the pipeline graph.'
    self.text.append(text)
    self.writeFormattedText()
    self.terminate()

  # If a a node attribute was requested, but the node does not have an associated attributes, terminate.
  def noAttributesInAttributeRequest(self, node):
    text = 'Requested attributes from a pipeline graph node with no assigned attributes.'
    self.text.append(text)
    text = 'A pipeline node attribute was requested (using function getGraphNodeAttribute), however, no attributes have been attached to node \'' + \
    node + '\'.'
    self.text.append(text)
    self.writeFormattedText()
    self.terminate()

  # If a a node attribute was requested, but the node does not have the requested attribute, terminate.
  def attributeNotAssociatedWithNodeNoGraph(self, attribute, nodeType, inTaskNode, inFileNode, inOptionsNode):
    text = 'Requested a non-existent attribute from a non-graph node.'
    self.text.append(text)
    text = 'A pipeline node attribute was requested (using function getGraphNodeAttributeNoGraph), however, the requested attribute \'' + \
    attribute + '\' is not associated with the supplied node.'
    self.text.append(text)
    self.text.append('\t')

    # Check the node types to see if the requested attribute is available for some node types.
    if inTaskNode or inFileNode or inOptionsNode:
      text = 'The supplied node is a ' + nodeType + ' and the requested attribute is only available in the following node types:'
      self.text.append(text)
      if inTaskNode: self.text.append('\ttask nodes')
      if inFileNode: self.text.append('\tfile nodes')
      if inOptionsNode: self.text.append('\toptions nodes')
    else:
      text = 'The requested attribute is not associated with any of the available node data structures.'
      node + '\'.'
      self.text.append(text)
    self.writeFormattedText()
    self.terminate()

  # If a a node attribute was requested, but the node does not have the requested attribute, terminate.
  def attributeNotAssociatedWithNode(self, node, attribute, nodeType, inTaskNode, inFileNode, inOptionsNode):
    text = 'Requested a non-existent attribute from a pipeline graph node.'
    self.text.append(text)
    text = 'A pipeline node attribute was requested (using function getGraphNodeAttribute), however, the requested attribute \'' + attribute + \
    '\' is not associated with the supplied node \'' + node + '\'.  '
    self.text.append(text)
    self.text.append('\t')

    # Check the node types to see if the requested attribute is available for some node types.
    if inTaskNode or inFileNode or inOptionsNode:
      text = 'The supplied node is a ' + nodeType + ' and the requested attribute is only available in the following node types:'
      self.text.append(text)
      if inTaskNode: self.text.append('\ttask nodes')
      if inFileNode: self.text.append('\tfile nodes')
      if inOptionsNode: self.text.append('\toptions nodes')
    else:
      text = 'The requested attribute is not associated with any of the available node data structures.'
      node + '\'.'
      self.text.append(text)
    self.writeFormattedText()
    self.terminate()

  # Information about the extension was requested for a task argument pair that hasn't been specified.
  def invalidExtensionRequest(self, task, argument, extensions):
    self.text.append('Invalid extension request.')
    self.text.append('The required extension for the task/argument \'' + task + ' ' + argument + '\' was requested, but the information ' + \
    'is not included in the pipeline configuration file. Please check and update the configuration file for this argument.')
    self.writeFormattedText()
    self.terminate()

  ################################################
  # Errors associated with setting node attributes
  ################################################

  # If a a node attribute was set, but the node does not exist in the graph, terminate.
  def missingNodeInAttributeSet(self, node):
    text = 'Attempt to set unknown pipeline graph node.'
    self.text.append(text)
    text = 'An attempt to set a pipeline node attribute was made (using function setGraphNodeAttribute), however, the requested node \'' + node + \
    '\' does not exist in the pipeline graph.'
    self.text.append(text)
    self.writeFormattedText()
    self.terminate()

  # If a a node attribute was set, but the node does not have an associated attributes, terminate.
  def noAttributesInAttributeSet(self, node):
    text = 'Attempt to set attributes from a pipeline graph node with no assigned attributes.'
    self.text.append(text)
    text = 'An attempt to set a pipeline node attribute was made (using function setGraphNodeAttribute), however, no attributes have been attached ' + \
    'to node \'' + node + '\'.'
    self.text.append(text)
    self.writeFormattedText()
    self.terminate()

  # If a a node attribute was set, but the node does not have the requested attribute, terminate.
  def attributeNotAssociatedWithNodeInSet(self, node, attribute, nodeType, inTaskNode, inFileNode, inOptionsNode):
    text = 'Attempt to set a non-existent attribute from a pipeline graph node.'
    self.text.append(text)
    text = 'An attempt to set a pipeline node attribute was made (using function setGraphNodeAttribute), however, the requested attribute \'' + \
    attribute + '\' is not associated with the supplied node \'' + node + '\'.  '
    self.text.append(text)
    self.text.append('\t')

    # Check the node types to see if the requested attribute is available for some node types.
    if inTaskNode or inFileNode or inOptionsNode:
      text = 'The supplied node is a ' + nodeType + ' and the requested attribute is only available in the following node types:'
      self.text.append(text)
      if inTaskNode: self.text.append('\ttask nodes')
      if inFileNode: self.text.append('\tfile nodes')
      if inOptionsNode: self.text.append('\toptions nodes')
    else:
      text = 'The requested attribute is not associated with any of the available node data structures.'
      self.text.append(text)
    self.writeFormattedText()
    self.terminate()

  # If a a node attribute was set, but the node does not have the requested attribute, terminate.  This is for setting
  # values in nodes not in the graph.
  def attributeNotAssociatedWithNodeInSetNoGraph(self, attribute, nodeType, inTaskNode, inFileNode, inOptionsNode):
    text = 'Attempt to set a non-existent attribute from a non-pipeline graph node.'
    self.text.append(text)
    text = 'An attempt to set a node attribute was made (using function setNodeAttribute), however, the requested attribute \'' + \
    attribute + '\' is not associated with the supplied node.'
    self.text.append(text)
    self.text.append('\t')

    # Check the node types to see if the requested attribute is available for some node types.
    if inTaskNode or inFileNode or inOptionsNode:
      text = 'The supplied node is a ' + nodeType + ' and the requested attribute is only available in the following node types:'
      self.text.append(text)
      if inTaskNode: self.text.append('\ttask nodes')
      if inFileNode: self.text.append('\tfile nodes')
      if inOptionsNode: self.text.append('\toptions nodes')
    else:
      text = 'The requested attribute is not associated with any of the available node data structures.'
      self.text.append(text)
    self.writeFormattedText()
    self.terminate()

  ################################################
  # Errors associated with getting edge attributes
  ################################################

  # Node is missing.
  def noNodeInGetEdgeAttribute(self, sourceNode, targetNode, missingNode):
    text = 'Attempt to get attribute for a non-existent edge.'
    self.text.append(text)
    text = 'An attempt to get information about an edge between nodes \'' + sourceNode + '\' and \'' + targetNode + '\' was made, however, node \''
    if missingNode == 'source': text += sourceNode
    elif missingNode == 'target': text += targetNode
    text += '\' does not exist.'
    self.text.append(text)
    self.writeFormattedText()
    self.terminate()

  # If the edge being interrogated has no attributes block.
  def noAttributesForEdge(self, sourceNode, targetNode):
    text = 'Attempt to get attribute for an edge with no attributes block.'
    self.text.append(text)
    text = 'An attempt to get information about an edge between nodes \'' + sourceNode + '\' and \'' + targetNode + '\' was made, however, this ' + \
    'edge does not have an attributes block defined.'
    self.text.append(text)
    self.writeFormattedText()
    self.terminate()

  # If the edge being interrogated has no attributes block.
  def invalidAttributeForEdge(self, sourceNode, targetNode, attribute):
    text = 'Attempt to get non-existent attribute for an edge.'
    self.text.append(text)
    text = 'An attempt to get information about an edge between nodes \'' + sourceNode + '\' and \'' + targetNode + '\' was made, however, the ' + \
    'requested attribute \'' + attribute + '\' is not associated with the edge.'
    self.text.append(text)
    self.writeFormattedText()
    self.terminate()

  ##############################################
  # Errors associated with setting attributes. #
  ##############################################

  # If the tool attributes for an invalid tool are set.
  def invalidToolInSetToolAttribute(self, tool):
    text = 'Attempt to set tool attributes for an invalid tool: ' + tool
    self.text.append(text)
    text = 'A call was made to a function (setToolAttribute) to set tool attributes.  The tool \'' + tool + '\' does not exist, so no ' + \
    'data can be set for this tool.'
    self.text.append(text)
    self.writeFormattedText()
    self.terminate()

  # If the tool attributes for an tool include an invalid attribute.
  def invalidAttributeInSetAttribute(self, attribute, isPipeline):
    runType = 'pipeline' if isPipeline else 'tool'
    self.text.append('Attempt to set an invalid attribute for ' + runType + '.')
    self.text.append('A call was made to a function (setAttribute) to set ' + runType + ' attributes.  The attribute \'' + attribute + \
    '\' does not exist, so this attribute cannot be set.')
    self.writeFormattedText()
    self.terminate()

  #######################################################
  # Errors associated with extracting tool information. #
  #######################################################

  #TODO NEEDED?
  # If data about an invalid tool is requested.
  def invalidTool(self, tool, function):
    text = 'Requested data about an invalid tool: ' + tool
    self.text.append(text)
    text = 'A call was made to a function (' + function + ') to extract tool information.  The tool \'' + tool + '\' does not exist, so no ' + \
    'data exists can be extracted.'
    self.text.append(text)
    self.writeFormattedText()
    self.terminate()
    
  #TODO NEEDED?
  # If no arguments data has been supplied for the tool.
  def noArgumentsInformation(self, tool, function):
    self.text.append('No arguments information for tool: ' + tool)
    text = 'A call was made to a function (' + function + ') to extract argument information for tool \'' + tool + '\'.  No argument ' + \
    'is available for this tool.'
    self.text.append(text)
    self.writeFormattedText()
    self.terminate()

  ################################################
  # Errors associated with pipeline information. #
  ################################################

  # An unknown command line argument was requested,
  def unknownPipelineArgument(self, argument):
    self.text.append('Unknown pipeline argument: ' + argument)
    self.text.append('The argument \'' + argument + '\' was included on the command line, but is not a valid argument for this pipeline.')
    self.writeFormattedText()
    self.terminate()

  def unsetFile(self, longFormArgument, shortFormArgument, description, validAlternatives, isPipeline):

    # If validAlternatives, contains no alternatives, set it to None.
    if validAlternatives:
      for counter, (longAlternative, shortAlternative) in enumerate(reversed(validAlternatives)):
        if not longAlternative and not shortAlternative: del validAlternatives[counter]

    self.text.append('The required command line argument ' + longFormArgument + ' (' + shortFormArgument + ') is missing.')
    if validAlternatives:
      self.text.append('This argument is described as \'' + description + '\' and can also be set by using one of the following arguments:')
      self.text.append('\t')
      for longAlternative, shortAlternative in validAlternatives: self.text.append(longAlternative + ' (' + shortAlternative + ')')
    else:
      self.text.append('This argument is described as \'' + description + '\'.')
    self.text.append('\t')
    if isPipeline:
      self.text.append('There is not pipeline argument available to set this argument. Please ensure that required arguments are available ' + \
      'as pipeline arguments, or set the tool argument using the syntax:')
      self.text.append('\t')
      self.text.append('gkno pipe <pipeline> --<tool> [' + longFormArgument + ' [value]].')
    else: self.text.append('Please ensure that all required arguments have been set.')
    self.writeFormattedText()
    self.terminate()

  ###########################################
  # Errors with exporting an parameter set. #
  ###########################################

  # If the user is attempting to export an parameter set and they have supplied a file for performing multiple
  # runs, terminate.
  def exportParameterSetForMultipleRuns(self):
    print(file=sys.stderr)
    self.text.append('Error in attempting to export parameter set file.')
    self.text.append('An parameter set can only be exported if gkno is being run without the \'--multiple-runs (-mr)\' command line argument. If ' + \
    'multiple runs are required, export the parameter set without the arguments included in the multiple runs selection, then use the parameter ' + \
    'set in conjuction with the \'--multiple-run (-mr)\' argument.')
    self.writeFormattedText()
    self.terminate()

  # Multiple parameter set names were provided.
  def exportParameterSetSetMultipleTimes(self, runName, isVerbose):
    if isVerbose: print(file=sys.stderr)
    self.text.append('Error in attempting to export parameter set file.')
    self.text.append('The command line argument \'--export-parameter-set (-ep)\' was set multiple times on the command line. When outputting an ' + \
    'parameter set, all of the supplied arguments and values are stored in the external parameter set file \'' + runName + \
    '_parameter sets.json\'. Only a single parameter set can be created at a time, so \'--export-parameter-set (-ep)\' can only appear once on ' + \
    'the command line.')
    self.writeFormattedText()
    self.terminate()

  # If the --export-parameter set argument has no value associated with it.
  def noParameterSetNameInExport(self, filename, value, isVerbose):
    if isVerbose: print(file=sys.stderr)
    self.text.append('Error in attempting to export parameter set file.')
    self.text.append('The --export-parameter-set (-ep) command line argument requires the desired parameter set name to be provided.  The ' + \
    'provided value (' + value + ') is either not a string or is missing.  When outputting a new parameter set, the file \'' + filename + \
    '\' will store the parameter set information with the supplied name.  Please check the command line for errors.')
    self.writeFormattedText()
    self.terminate()

  # If the --export-parameter set argument requests an parameter set name that already exists, terminate.
  def parameterSetNameExists(self, parameterSetName, isVerbose):
    if isVerbose: print(file=sys.stderr)
    self.text.append('Requested parameter set name already exists: ' + parameterSetName)
    self.text.append('The command line argument \'--export-parameter-set (-ep)\' sets the name of the parameter set to be exported.  The ' + \
    'requested name \'' + parameterSetName + '\' is already defined, either in the configuration file or the parameter sets file.  Please ' + \
    'select a different name for the parameter set to be exported.')
    self.writeFormattedText()
    self.terminate()

  # If no arguments were provided for the parameter set.
  def noInformationProvidedForParameterSet(self, isVerbose):
    if isVerbose: print(file=sys.stderr)
    self.text.append('No information provided for parameter set.')
    self.text.append('The \'--export-parameter-set (-ep) command line argument was set, indicating that an parameter set is to be created. No ' + \
    'other information was provided on the command line, however, so the parameter set would be empty. Please provide some information for ' + \
    'the parameter set.')
    self.writeFormattedText()
    self.terminate()

  #################################################################
  # Errors with merging the nodes and generating the final graph. #
  #################################################################

  # Failure to find a node with the required extension.
  def noFileNodeIDWithCorrectExtension(self, task, argument, extension):
    self.text.append('Error with file nodes.')
    self.text.append('The task \'' + task + '\' has the argument \'' + argument + '\' which is linked to other tasks in the pipeline graph. ' + \
    'The file node is associated with a filename stub. The expected extension \'' + extension + '\' is not associated with any of the files ' + \
    'from the filename stub, so no edge can be created between this task and the required file. Please check the pipeline configuration file ' + \
    'for errors associated with this task/argument.')
    self.writeFormattedText()
    self.terminate()

  ##############################
  # Terminate configurationClass
  ##############################

  def terminate(self):
    print(file=sys.stderr)
    print('================================================================================================', file=sys.stderr)
    print('  TERMINATED: Errors in configurationClass.  See specific error messages above for resolution.', file=sys.stderr)
    print('================================================================================================', file=sys.stderr)

    # If in debugging mode, list where the errors came from.
    if self.isDebug:
      frameInfo = getframeinfo(currentframe().f_back, 2)
      print()
      print(frameInfo.filename, ' line ', frameInfo.lineno, ': ', sep = '', file = sys.stdout)
      previousFrameInfo = getframeinfo(getouterframes(currentframe())[2][0])
      print(previousFrameInfo.filename, previousFrameInfo.lineno, previousFrameInfo.function)

    # Terminate.
    exit(2)
