#!/usr/bin/python

from __future__ import print_function

import os
import sys

class configurationClassErrors:

  # Initialise.
  def __init__(self):
    self.hasError  = False
    self.errorType = 'ERROR'
    self.text      = []

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
    text = 'The configuration file for ' + runType + '\'' + name + '\' contains the general attribute \'' + attribute + '\'. This is an ' + \
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

  ##########################################
  # Errors with a tool configuration file. #
  ##########################################

  # Attribute has the wrong type.
  def incorrectTypeInToolConfigurationFile(self, name, attribute, argument, value, expectedType, isPipeline):

    # Find the given type.
    isType    = self.findType(type(value))
    needsType = self.findType(expectedType)
    if isType == None: isType = 'Unknown'
    if needsType == None: needsType = 'Unknown'
    self.text.append('Invalid attribute value in tool configuration file.')
    text = 'The attribute \'' + str(attribute) + '\' in the configuration file for \'' + str(name) + '\''
    text += ', argument \'' + argument + '\' ' if argument else ' '
    if isType == 'list' or isType == 'dictionary':  text += 'is given a value '
    else: text += 'is given the value \'' + str(value) + '\'. This value is '
    text += 'of an incorrect type (' + isType + '); the expected type is \'' + needsType + '\'. Please correct ' + \
    'this value in the configuration file.'
    self.text.append(text)
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
  def noLongFormForToolArgument(self, tool):
    self.text.append('Missing long form for tool argument in configuration file')
    self.text.append('All arguments defined in a tool configuration file, must have a long and short form defined. The long form version is' + \
    'used to identify the argument in all of the relevant data structures as well as for identifying problematic arguments in error messages. ' + \
    'Please check the configuration file for tool \'' + tool + '\' and ensure that the \'long form argument\' attribute is present for all ' + \
    'arguments.')
    self.writeFormattedText()
    self.terminate()

  # An argument attribute in the configuration file is invalid.
  def invalidArgumentAttributeInToolConfigurationFile(self, tool, argument, attribute, allowedAttributes):
    self.text.append('Invalid argument attribute in tool configuration file: ' + attribute)
    text = 'The configuration file for tool \'' + tool + '\' contains the argument \'' + argument + '\'. This argument contains the ' + \
    'attribute \'' + attribute + '\', which is not recognised. The argument attributes allowed in a tool configuration file are:'
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

  # An argument attribute in the configuration file is missing.
  def missingArgumentAttributeInToolConfigurationFile(self, tool, argument, attribute, allowedAttributes):
    self.text.append('Missing attribute in tool configuration file for argument: ' + argument)
    text = 'The configuration file for tool \'' + tool + '\', argument \'' + argument + '\' is missing the attribute \'' + attribute + '\'. The ' + \
    'following attributes are required for each argument in a tool configuration file:'
    self.text.append(text)
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

  # Given a value, return a string representation of the data type.
  def findType(self, providedType):
    if providedType == str: return 'string'
    elif providedType == int: return 'integer'
    elif providedType == float: return 'float'
    elif providedType == bool: return 'Boolean'
    elif providedType == dict: return 'dictionary'
    elif providedType == list: return 'list'
    else: return None

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
    self.text.append('Unknown argument: ' + argument)
    text = 'The argument \'' + argument + '\' was included on the command line, but is not a valid argument for the current tool (' + \
    tool + ').'
    self.text.append(text)
    self.writeFormattedText()
    self.terminate()

  ##############################################
  # Errors with a pipeline configuration file. #
  ##############################################

  # Attribute has the wrong type.
  def incorrectTypeInPipelineConfigurationFile(self, pipeline, attribute, value, expectedType):

    # Find the given type.
    isType    = self.findType(type(value))
    needsType = self.findType(expectedType)
    if isType == None: isType = 'Unknown'
    if needsType == None: needsType = 'Unknown'
    self.text.append('Invalid attribute value in pipeline configuration file.')
    text = 'The attribute \'' + str(attribute) + '\' in the configuration file for pipeline \'' + str(pipeline) + '\' '
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

  # A pipeline node links a filename stub argument with a non-filename stub argument and the extension
  # to use is not specified.
  def missingExtensionForNonStub(self, nodeID, stubArguments, nonStubArguments):
    self.text.append('Missing extension in pipeline configuration file node: ' + nodeID)
    self.text.append('The node \'' + nodeID + '\' in the pipeline configuration file links arguments from different tasks in the pipeline. ' + \
    'There is at least one filename stub argument and one non filename stub argument linked together in this node. The filename stub argument ' + \
    'is associated with multiple files, each with a different extension, so in order to link this to another argument, the required extension ' + \
    'must be specified in this node to ensure that the correct files are passed through the pipeline workflow. The task/arguments associated ' + \
    'with a filename stub are:')
    self.text.append('\t')
    for task, argument in stubArguments: self.text.append('Task: \'' + task + '\', argument: \'' + argument + '\'')
    self.text.append('\t')
    self.text.append('The task/arguments associated with single files (not filename stubs) are:')
    self.text.append('\t')
    for task, argument in nonStubArguments: self.text.append('Task: ' + task + ', argument: ' + argument)
    self.writeFormattedText()
    self.terminate()

  ###########################################################
  # Errors associated with instances in configuration file. #
  ###########################################################

  # Requested instance does not exist.
  def missingInstance(self, name, instanceName, isPipeline, availableInstances):
    runType = 'pipeline' if isPipeline else 'tool'
    self.text.append('Requested instance for ' + runType + ' \'' + name + '\' does not exist: ' + instanceName)
    self.text.append('The instance \'' + instanceName + '\' was requested, but no instance with this name is available in the ' + runType + \
    ' configuration file or the external instances configuration file for the ' + runType + '. The instances available for this ' + runType + \
    ' are:')
    self.text.append('\t')
    for instance in availableInstances: self.text.append(instance)
    self.writeFormattedText()
    self.terminate()

  # Instance has no ID.
  def noIDForInstance(self, runName, isPipeline):
    runType = 'pipeline' if isPipeline else 'tool'
    self.text.append('Instance has no ID')
    self.text.append('The instances section of the configuration file for ' + runType + ' configuration file \'' + runName + '\' contains ' + \
    'an instance with no ID. All instances must have a unique identifier. Please check the configuration file and ensure that all instances ' + \
    'are correctly formed with a valid ID.')
    self.writeFormattedText()
    self.terminate()

  # Missing attribute.
  def missingAttributeInInstance(self, runName, attribute, allowedAttributes, instanceID, isPipeline):
    runType = 'pipeline' if isPipeline else 'tool'
    self.text.append('Missing attribute in instances section of ' + runType + ' configuration file: ' + runName)
    self.text.append('The ' + runType + ' configuration file for \'' + runName + '\' is missing the instances attribute \'' + attribute + \
    '\' from instance \'' + instanceID + '\'. The following attributes are required in the instances section of the pipeline configuration file:')
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

  # Invalid attribute in instance.
  def invalidAttributeInInstance(self, pipeline, instance, attribute, allowedAttributes):
    self.text.append('Invalid attribute in the instances section of the configuration file for pipeline: ' + pipeline)
    text = 'The \'instances\' section contains information for the instance \'' + instance + '\' in the configuration file for pipeline \'' + \
    pipeline + '\'. This instance contains the attribute \'' + attribute + '\'. This is an unrecognised attribute which is not permitted. The ' + \
    'attributes allowed in the instances section of the pipeline configuration file are:'
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

  # Invalid attribute in instance node.
  def invalidAttributeInInstanceNode(self, runName, instance, attribute, allowedAttributes, isPipeline):
    runType = 'pipeline' if isPipeline else 'tool'
    self.text.append('Invalid attribute in a node in the instances section of the configuration file for ' + runType + ': ' + runName)
    self.text.append('The \'instances\' section in the ' + runType + ' configuration file \'' + runName + '\' contains information for ' + \
    'the instance \'' + instance + '\'. This instance contains the attribute \'' + attribute + '\' in one of the instance nodes. This is an ' + \
    'unrecognised attribute which is not permitted. The attributes allowed for each node in the instances section of the ' + runType + \
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
  def missingAttributeInPipelineInstanceNode(self, runName, instance, attribute, allowedAttributes, isPipeline):
    runType = 'pipeline' if isPipeline else 'tool'
    self.text.append('Missing attribute in instances section of ' + runType + ' configuration file: ' + runName)
    self.text.append('The ' + runType + ' configuration file for \'' + runName + '\' contains a section \'instances\' for defining preset ' + \
    'values for the ' + runType + '. One of the nodes in the instance \'' + instance + '\' is missing the attribute \'' + attribute + '\'. ' + \
    'The following attributes are required for each node in the \'instances\' section of the ' + runType + ' configuration file:')
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

  # Duplicated instance.
  def duplicateInstance(self, runName, instanceID, isPipeline):
    runType = 'pipeline' if isPipeline else 'tool'
    self.text.append('Duplicated instance information for ' + runType + ': ' + runName)
    self.text.append('Each instance appearing in the instances section of the configuration file must be unique. This includes any ' + \
    'instances included in the external instance file \'' + runName + '_instances\'. The instance \'' + instanceID + '\' appears multiple ' + \
    'times for this ' + runType + '. Please ensure that all instance names are unique.')
    self.writeFormattedText()
    self.terminate()

  # Duplicate attribute in instance nodes.
  def duplicateNodeInInstance(self, runName, instanceID, attributeType, attribute, isPipeline):
    runType = 'pipeline' if isPipeline else 'tool'
    self.text.append('Duplicated instance attribute for ' + runType + ': ' + runName)
    self.text.append('Each instance contains a set of nodes defining the values to be given to certain arguments. Within any one instance, ' + \
    'each ID and argument must be unique. The ' + attributeType + ' \'' + attribute + '\' appears multiple times in the ' + runType + \
    ' instance \'' + instanceID + '\'. Please ensure that each node for each instance in the configuration file contains a unique ID and ' + \
    'argument.')
    self.writeFormattedText()
    self.terminate()

  # Invalid argument in instance.
  def invalidArgumentInToolInstance(self, runName, instanceName, argumentID, argument):
    self.text.append('Invalid argument in instance: ' + instanceName)
    self.text.append('The configuration file contains the instance \'' + instanceName + '\', which  contains information for the argument \'' + \
    argument + '\' (ID: \'' + argumentID + '\'). This argument is not a valid argument for the tool. Please ensure that all arguments defined in ' + \
    'the instances section are valid for the tool.')
    self.writeFormattedText()
    self.terminate()

  #######################################
  # Errors associated with node values. #
  #######################################

  ###################################################
  # Errors associated with getting node attributes. *
  ###################################################

  # If a a node attribute was requested, but the node does not exist in the graph, terminate.
  def missingNodeInAttributeRequest(self, node):
    self.text.append('Unknown pipeline graph node attribute requested.')
    text = 'A pipeline node attribute was requested (using function getGraphNodeAttribute), however, the requested node \'' + node + \
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

  ############################################
  # Errors associated with setting attributes.
  ############################################

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

  ####################################################
  # Errors associated with extracting tool information
  ####################################################

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
    text = 'The argument \'' + argument + '\' was included on the command line, but is not a valid argument for this pipeline.'
    self.text.append(text)
    self.writeFormattedText()
    self.terminate()

  def unsetFile(self, pipelineLongFormArgument, pipelineShortFormArgument, description):
    self.text.append('The required command line argument ' + pipelineLongFormArgument + ' (' + pipelineShortFormArgument + ') is missing.')
    self.text.append('This argument is described as the following: ' + description)
    self.text.append('\t')
    self.text.append('Check the usage information for all required arguments.')
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
    exit(2)