#!/bin/bash/python

from __future__ import print_function
import networkx as nx
from copy import deepcopy

import configurationClassErrors
from configurationClassErrors import *

import nodeAttributes
from nodeAttributes import *

import edgeAttributes
from edgeAttributes import *

import toolAttributes
from toolAttributes import *

import json
import os
import sys

# Define a class to store general pipeline attributes,
class pipelineAttributes:
  def __init__(self):

    # Provide a description of the pipeline.
    self.description = None

    # As with the tools, untested pipelines can be listed as experimental until they
    # have been refined. This ensures that  users are aware that the pipeline should
    # be used with caution.
    self.isDevelopmental = False

    # If the pipeline should be hidden in the help.
    self.isHiddenInHelp = False

    # Define the help groups to which pipelines belong.
    self.helpGroup = 'General'

# Define a class to store task attribtues.
class taskAttributes:
  def __init__(self):
    self.tool         = {}
    self.outputStream = False

# Define a class to store information on pipeline nodes.
class pipelineNodeAttributes:
  def __init__(self):
    self.description       = None
    self.extensions        = None
    self.deleteFiles       = False
    self.ID                = None
    self.greedyTasks       = None
    self.isRequired        = False
    self.longFormArgument  = None
    self.shortFormArgument = None
    self.tasks             = None

    # Store information about evaluating a command.
    self.evaluateCommand = None

# Define a class to store information for evaluating commands at run-time.
class evaluateCommandAttributes:
  def __init__(self):

    # Define the command to be evaluated.
    self.command = None

    # Store the values to be added to the command.
    self.values = {}

# Define a class to store pipeline argument attributes.
class argumentAttributes:
  def __init__(self):

    # A description of the argument.
    self.description = None

    # If the argument is required by the pipeline, even if not by the tool.
    self.isRequired = False

    #
    self.ID = None

    # The node ID in the configuration file.
    self.configNodeID = None

    # The arguments short form.
    self.shortFormArgument = None

class pipelineConfiguration:
  def __init__(self):

    # Define the attributes for a pipeline.
    self.attributes = {}

    # Define the task attributes.
    self.taskAttributes = {}

    # Define the node attributes.
    self.nodeAttributes = {}

    # Define commands to be evaluated at run time.
    self.evaluateCommands = {}

    # Define the errors class for handling errors.
    self.errors = configurationClassErrors()

    # Define a structure to store pipeline argument information.
    self.pipelineArguments = {}

    # Define a structure to store which tasks and arguments are common to a node. For
    # example, if two tasks use the same input file, they appear in the configuration
    # file in the same node in the tasks (or greedy tasks) section. Duplicate the
    # greedy tasks in their own data structure,
    self.commonNodes = {}
    self.greedyTasks = {}

    # Define the pipeline workflow.
    self.workflow = []

    # Define a dictionary to store information about extensions.
    self.linkedExtension = {}

    # Define a structure that links the task and argument described in the 'tasks' list in
    # the nodes section with the pipeline argument. Also define the reverse, that provides
    # a list of all tasks and respective arguments for each pipeline argument.
    self.taskArgument           = {}
    self.pipelineToTaskArgument = {}

    # Keep track of tasks that output to the stream
    self.tasksOutputtingToStream = {}

    # Define a structure to keep track of which task arguments are linked to others.
    self.linkedTaskArguments = {}

    # Define a variable to determine whether termination should result if an error in a
    # configuration file is found.
    self.allowTermination = True

    # Define the methods to operate on the graph nodes and edges,
    self.edgeMethods = edgeClass()
    self.nodeMethods = nodeClass()

    #TODO REQUIRED?
    self.filename            = ''
    self.pipelineName        = ''

  #TODO
  # Validate the contents of the tool configuration file.
  def processConfigurationData(self, data, pipeline, toolFiles, allowTermination):

    # Set the allowTermination variable. Each of the following subroutines check different
    # aspects of the configuration file. If problems are found, termination will result with
    # an error message unless allowTermination is set to False.
    self.allowTermination = allowTermination
    success               = True

    # Parse the pipeline configuration file and check that all fields are valid. Ensure that there are
    # no errors, omissions or inconsistencies. Store the information in the relevant data structures
    # as the checks are performed.
    #
    # Check the general tool information.
    success, self.attributes = self.checkGeneralAttributes(pipeline, data)

    # Check the 'tasks' section of the configuration file.
    if success: success = self.checkTasks(pipeline, data['tasks'], toolFiles)

    # Check the contents of the nodes section.
    if success: success = self.checkNodes(pipeline, data['nodes'])

    # Check evaluate commands information.
    if success: success = self.checkEvaluateCommands(pipeline)

    # Check and store the pipeline arguments.
    if success: self.setPipelineArguments()

    # From the node data, define which arguments are greedy.
    if success: success = self.getNodeTasks(pipeline)

    return success

  def checkGeneralAttributes(self, pipeline, data):

    # Set the general tool attributes.
    attributes = pipelineAttributes()

    # Define the allowed general attributes.
    allowedAttributes                   = {}
    allowedAttributes['description']    = (str, True, True, 'description')
    allowedAttributes['developmental']  = (bool, False, True, 'isDevelopmental')
    allowedAttributes['help group']     = (str, False, True, 'helpGroup')
    allowedAttributes['hide in help']   = (bool, False, True, 'isHiddenInHelp')
    allowedAttributes['parameter sets'] = (list, True, False, None)
    allowedAttributes['nodes']          = (list, True, False, None)
    allowedAttributes['tasks']          = (dict, True, False, None)

    # Keep track of the observed required values.
    observedAttributes = {}

    # Loop over all of the attributes in the configuration file.
    for attribute in data:

      # If the value is not in the allowedAttributes, it is not an allowed value and execution
      # should be terminate with an error.
      if attribute not in allowedAttributes:
        if self.allowTermination: self.errors.invalidGeneralAttributeInConfigurationFile(pipeline, attribute, allowedAttributes, True)
        else: return False, attributes

      # Mark this values as having been observed,
      observedAttributes[attribute] = True

      # Check that the value given to the attribute is of the correct type. If the value is unicode,
      # convert to a string first.
      value = str(data[attribute]) if isinstance(data[attribute], unicode) else data[attribute]
      if allowedAttributes[attribute][0] != type(value):
        if self.allowTermination:
          self.errors.incorrectTypeInPipelineConfigurationFile(pipeline, attribute, value, allowedAttributes[attribute][0], 'general')
        else: return False, attributes

      # At this point, the attribute in the configuration file is allowed and of valid type. Check that 
      # the value itself is valid (if necessary) and store the value.
      if allowedAttributes[attribute][2]: self.setAttribute(attributes, allowedAttributes[attribute][3], value)

    # Having parsed all of the general attributes attributes, check that all those that are required
    # are present.
    for attribute in allowedAttributes:
      if allowedAttributes[attribute][1] and attribute not in observedAttributes:
        if self.allowTermination: self.errors.missingGeneralAttributeInConfigurationFile(pipeline, attribute, allowedAttributes, True)
        return False, attributes

    return True, attributes

  # Check the 'tasks' section of the configuration file.
  def checkTasks(self, pipeline, tasks, toolFiles):

    # Define the allowed general attributes.
    allowedAttributes                     = {}
    allowedAttributes['tool']             = (str, True, True, 'tool')
    allowedAttributes['output to stream'] = (bool, False, True, 'outputStream')

    for task in tasks:

      # Define the taskAttributes object.
      attributes = taskAttributes()

      # Keep track of the observed required values.
      observedAttributes = {}

      # Check that the task name is accompanied by a dictionary.
      if not isinstance(tasks[task], dict):
        if self.allowTermination: self.errors.taskIsNotDictionary(pipeline, task)
        else: return False

      # Loop over the included attributes.
      for attribute in tasks[task]:
        if attribute not in allowedAttributes:
          if self.allowTermination: self.errors.invalidAttributeInTasks(pipeline, task, attribute, allowedAttributes)
          return False

        # Check that the value given to the attribute is of the correct type. If the value is unicode,
        # convert to a string first.
        value = str(tasks[task][attribute]) if isinstance(tasks[task][attribute], unicode) else tasks[task][attribute]
        if allowedAttributes[attribute][0] != type(value):
          if self.allowTermination:
            self.errors.incorrectTypeInPipelineConfigurationFile(pipeline, attribute, value, allowedAttributes[attribute][0], 'tasks')
          else: return False

        # Mark the attribute as seen.
        observedAttributes[attribute] = True

        # Store the given attribtue.
        if allowedAttributes[attribute][2]: self.setAttribute(attributes, allowedAttributes[attribute][3], tasks[task][attribute])

      # Having parsed all of the general attributes attributes, check that all those that are required
      # are present.
      for attribute in allowedAttributes:
        if allowedAttributes[attribute][1] and attribute not in observedAttributes:
          if self.allowTermination: self.errors.missingAttributeInPipelineConfigurationFile(pipeline, attribute, allowedAttributes, 'tasks', None)
          else: return False

      # Check that each task has a tool defined and that a tool configuration file exists for this tool.
      tool = tasks[task]['tool']
      if tool + '.json' not in toolFiles:
        if self.allowTermination: self.errors.invalidToolInPipelineConfigurationFile(pipeline, task, tool)
        else: return False

      # Store the attributes for the task.
      self.taskAttributes[task] = attributes

    return True

  # Check the contents of the nodes section.
  def checkNodes(self, pipeline, nodes):

    # Define the allowed nodes attributes.
    allowedAttributes                        = {}
    allowedAttributes['ID']                  = (str, True, True, 'ID')
    allowedAttributes['description']         = (str, True, True, 'description')
    allowedAttributes['evaluate command']    = (dict, False, True, 'evaluateCommand')
    allowedAttributes['extensions']          = (dict, False, True, 'extensions')
    allowedAttributes['greedy tasks']        = (dict, False, True, 'greedyTasks')
    allowedAttributes['delete files']        = (bool, False, True, 'deleteFiles')
    allowedAttributes['long form argument']  = (str, False, True, 'longFormArgument')
    allowedAttributes['required']            = (bool, False, True, 'isRequired')
    allowedAttributes['short form argument'] = (str, False, True, 'shortFormArgument')
    allowedAttributes['tasks']               = (dict, True, True, 'tasks')

    # Loop over all of the defined nodes.
    for node in nodes:

      # Check that node is a dictionary.
      if not isinstance(node, dict):
        if self.allowTermination: self.errors.nodeIsNotADictionary(pipeline)
        else: return False

      # Define the attributes object.
      attributes = pipelineNodeAttributes()

      # Keep track of the observed required values.
      observedAttributes = {}

      # Check that the node has an ID. This will be used to identify the node in error messages.
      try: ID = node['ID']
      except: 
        if self.allowTermination: self.errors.noIDInPipelineNode(pipeline)
        else: return False

      # Loop over all attributes in the node.
      for attribute in node:
        if attribute not in allowedAttributes:
          if self.allowTermination: self.errors.invalidAttributeInNodes(pipeline, ID, attribute, allowedAttributes)
          else: return False

        # Check that the value given to the attribute is of the correct type. If the value is unicode,
        # convert to a string first.
        value = str(node[attribute]) if isinstance(node[attribute], unicode) else node[attribute]
        if allowedAttributes[attribute][0] != type(value):
          if self.allowTermination:
            self.errors.incorrectTypeInPipelineConfigurationFile(pipeline, attribute, value, allowedAttributes[attribute][0], 'nodes')
          else: return False

        # Mark the attribute as seen.
        observedAttributes[attribute] = True

        # Store the given attribtue.
        if allowedAttributes[attribute][2]: self.setAttribute(attributes, allowedAttributes[attribute][3], node[attribute])

      # Having parsed all of the general attributes attributes, check that all those that are required
      # are present.
      for attribute in allowedAttributes:
        if allowedAttributes[attribute][1] and attribute not in observedAttributes:
          if self.allowTermination: self.errors.missingAttributeInPipelineConfigurationFile(pipeline, attribute, allowedAttributes, 'nodes', ID)
          else: return False

      # Store the attributes.
      self.nodeAttributes[ID] = attributes

    return True

  # Check evaluate commands information.
  def checkEvaluateCommands(self, pipeline):

    # Define the allowed nodes attributes.
    allowedAttributes              = {}
    allowedAttributes['command']   = (str, False, True, '')
    allowedAttributes['add values'] = (dict, False, False, '')

    # Loop over all of the defined nodes.
    for ID in self.nodeAttributes:
      if self.nodeAttributes[ID].evaluateCommand:

        # Get the task/argument pair whose value is to be set as the evaulation of a command.
        taskList = self.nodeAttributes[ID].tasks

        # Check that the task/argument pair has not already had a command assigned.
        for task in taskList:
          argument = taskList[task]
          if task in self.evaluateCommands:
            if argument in self.evaluateCommands[task]:
              if self.allowTermination: self.errors.multipleEvaluationsForArgument(pipeline, ID, task, argument)
              else: return False

        # Keep track of the observed required values.
        observedAttributes = {}

        # Loop over all the attributes.
        for attribute in self.nodeAttributes[ID].evaluateCommand:
          value = self.nodeAttributes[ID].evaluateCommand[attribute]

          # Check that the attribute is valid.
          if attribute not in allowedAttributes:
            if self.allowTermination: self.errors.invalidAttributeInEvaluateCommand(pipeline, ID, attribute, allowedAttributes)
            else: return False

          # Record that the attribute was observed.
          observedAttributes[attribute] = True

        # Having parsed all of the general attributes attributes, check that all those that are required
        # are present.
        for attribute in allowedAttributes:
          if allowedAttributes[attribute][1] and attribute not in observedAttributes:
            if self.allowTermination: self.errors.missingAttributeInEvaluateCommand(pipeline, ID, attribute, allowedAttributes)
            else: return False

        # If the add values attribute is present, check that the task/argument it points to are valid
        # and that the ID associated with the value is present in the command and is unique.
        observedIDs = []
        for valueDictionary in self.nodeAttributes[ID].evaluateCommand['add values']:
          success, observedID = self.checkEvaluateCommandValues(pipeline, ID, valueDictionary, self.nodeAttributes[ID].evaluateCommand['command'])
          if observedID:
            if observedID in observedIDs:
              if self.allowTermination: self.errors.nonUniqueEvaluateCommandID(pipeline, ID, observedID)
              else: return False

            # Store the observedID.
            observedIDs.append(observedID)

        # Store the command information.
        for task in taskList:
          argument = taskList[task]
          attributes         = evaluateCommandAttributes()
          attributes.command = self.nodeAttributes[ID].evaluateCommand['command']
          attributes.values  = {}
          for valueDictionary in self.nodeAttributes[ID].evaluateCommand['add values']:
            attributes.values[valueDictionary['ID']] = (valueDictionary['task'], valueDictionary['argument'])

          if task not in self.evaluateCommands: self.evaluateCommands[task] = {}
          self.evaluateCommands[task][argument] = attributes

    return True

  # Check the values supplied for evaluate commands.
  def checkEvaluateCommandValues(self, pipeline, ID, dictionary, command):

    # Define the allowed attributes.
    allowedAttributes             = {}
    allowedAttributes['argument'] = (str, True)
    allowedAttributes['ID']       = (str, True)
    allowedAttributes['task']     = (str, True)

    # Keep track of the observed required values.
    observedAttributes = {}

    # Loop through the attributes.
    for attribute in dictionary:
      value = dictionary[attribute]

      # Check that the attribute is valid.
      if attribute not in allowedAttributes:
        if self.allowTermination: self.errors.invalidAttributeInEvaluateCommandValues(pipeline, ID, attribute, allowedAttributes)
  
      # Record that the attribute was observed.
      observedAttributes[attribute] = True

    # Having parsed all of the general attributes attributes, check that all those that are required
    # are present.
    for attribute in allowedAttributes:
      if allowedAttributes[attribute][1] and attribute not in observedAttributes:
        if self.allowTermination: self.errors.missingAttributeInEvaluateCommandValues(pipeline, ID, attribute, allowedAttributes)
        else: return False, ''

    # Having determine that the 'add values' section is complete and valid, check that the task is valid and that the
    # ID defining the value is present in the command. Checking that the argument is valid is performed later, when all
    # of the tool configuration files have been evaluated.
    if dictionary['task'] not in self.taskAttributes.keys():
      if self.allowTermination: self.errors.unknownTaskInEvaluateCommandValues(pipeline, ID, dictionary['task'])
      else: return False, ''

    # Check that the ID is in the command.
    if dictionary['ID'] not in command:
      if self.allowTermination: self.errors.unknownIDInEvaluateCommandValues(pipeline, ID, dictionary['ID'])
      else: return False, ''

    return True, dictionary['ID']

  # Check the validity and completeness of the pipeline argument definitions.
  def setPipelineArguments(self):
    observedLongFormArguments  = []
    observedShortFormArguments = []

    # Loop over all of the nodes and set the pipeline arguments.
    for nodeID in self.nodeAttributes:

      # The long form argument will be used as the key in this dictionary.
      longFormArgument = self.nodeAttributes[nodeID].longFormArgument

      # Set the other attributes only if the long form argument is present.
      if longFormArgument != None:
        shortFormArgument = self.nodeAttributes[nodeID].shortFormArgument

        # Check that the long forma rgument hasn't already been seen in the pipeline configuration
        # file. All command line arguments must be unique.
        if longFormArgument in observedLongFormArguments: self.errors.nonUniquePipelineFormArgument(nodeID, longFormArgument, shortFormArgument, True)
        if shortFormArgument in observedShortFormArguments: self.errors.nonUniquePipelineFormArgument(nodeID, longFormArgument, shortFormArgument, False)
        observedLongFormArguments.append(longFormArgument)
        observedShortFormArguments.append(shortFormArgument)

        # Define the structure to hold the argument information for this pipeline.
        attributes = argumentAttributes()

        self.setAttribute(attributes, 'description', self.nodeAttributes[nodeID].description)
        self.setAttribute(attributes, 'configNodeID', nodeID)
        self.setAttribute(attributes, 'shortFormArgument', shortFormArgument)
        self.setAttribute(attributes, 'isRequired', self.nodeAttributes[nodeID].isRequired)
  
        # Store the information.
        self.pipelineArguments[longFormArgument] = attributes

  # Go through all of the tasks (including greedy tasks) and ensure that the given tasks are
  # tasks in the pipeline. Arguments associated with the tasks are checked after the tool
  # configuration files have been processed.
  def getNodeTasks(self, pipeline):
    observedArguments = {}

    # Loop over all of the nodes.
    for configNodeID in self.nodeAttributes:
      self.commonNodes[configNodeID] = []

      numberOfTasks       = len(self.nodeAttributes[configNodeID].tasks) if self.nodeAttributes[configNodeID].tasks else 0
      numberOfGreedyTasks = len(self.nodeAttributes[configNodeID].greedyTasks) if self.nodeAttributes[configNodeID].greedyTasks else 0
      isLinked = True if (numberOfTasks + numberOfGreedyTasks) > 1 else False

      # Parse the tasks.
      if self.nodeAttributes[configNodeID].tasks:
        for task in self.nodeAttributes[configNodeID].tasks:

          # Check that the task is valid.
          if task not in self.taskAttributes.keys():
            if self.allowTermination: self.errors.invalidTaskInNode(pipeline, configNodeID, task, False)
            else: return False

          # Link the pipeline argument to the task/arguments listed with the node.
          taskArgument     = self.nodeAttributes[configNodeID].tasks[task]
          longFormArgument = self.nodeAttributes[configNodeID].longFormArgument
          if task not in self.taskArgument: self.taskArgument[task] = {}

          # If there is no long form argument, store None in taskArguments and nothing in pipelineToTaskArgument.
          if longFormArgument:
            self.taskArgument[task][str(taskArgument)] = str(longFormArgument)
            if str(longFormArgument) not in self.pipelineToTaskArgument: self.pipelineToTaskArgument[str(longFormArgument)] = []
            self.pipelineToTaskArgument[str(longFormArgument)].append((str(task), str(taskArgument)))
          else: self.taskArgument[task][str(taskArgument)] = None

          # Store the task and argument.
          self.commonNodes[configNodeID].append((str(task), str(taskArgument)))

          # Store the task/argument pair in the observedOptions dictionary. If this task/argument pair has already been seen
          if str(task) not in observedArguments: observedArguments[str(task)] = {}
          if str(taskArgument) not in observedArguments[str(task)]: observedArguments[str(task)][str(taskArgument)] = []
          observedArguments[str(task)][str(taskArgument)].append(str(configNodeID))

          # If the taskArgument is linked to another argument, store this in the pipelineArguments structure.
          if isLinked:
            if task not in self.linkedTaskArguments: self.linkedTaskArguments[task] = []
            if taskArgument not in self.linkedTaskArguments[task]: self.linkedTaskArguments[task].append(taskArgument)

      # Then parse the greedy tasks.
      if self.nodeAttributes[configNodeID].greedyTasks:
        for task in self.nodeAttributes[configNodeID].greedyTasks:

          # Check that the task is valid.
          if task not in self.taskAttributes.keys():
            if self.allowTermination: self.errors.invalidTaskInNode(pipeline, configNodeID, task, True)
            else: return False

          # Link the pipeline argument to the task/arguments listed with the node.
          if task not in self.taskArgument: self.taskArgument[task] = {}
          taskArgument     = self.nodeAttributes[configNodeID].greedyTasks[task]
          longFormArgument = self.nodeAttributes[configNodeID].longFormArgument
          if longFormArgument:
            self.taskArgument[task][str(taskArgument)] = self.nodeAttributes[configNodeID].longFormArgument
            if str(longFormArgument) not in self.pipelineToTaskArgument: self.pipelineToTaskArgument[str(longFormArgument)] = []
            self.pipelineToTaskArgument[str(longFormArgument)].append((str(task), str(taskArgument)))
          else: self.taskArgument[task][str(taskArgument)] = None

          # Store the task and argument.
          self.commonNodes[configNodeID].append((str(task), str(taskArgument)))
          if task not in self.greedyTasks: self.greedyTasks[task] = []
          self.greedyTasks[task].append(str(taskArgument))

          # Store the task/argument pair in the observedOptions dictionary. If this task/argument pair has already been seen
          if str(task) not in observedArguments: observedArguments[str(task)] = {}
          if str(taskArgument) not in observedArguments[str(task)]: observedArguments[str(task)][str(taskArgument)] = []
          observedArguments[str(task)][str(taskArgument)].append(str(configNodeID))

          # If the taskArgument is linked to another argument, store this in the pipelineArguments structure.
          if isLinked:
            if task not in self.linkedTaskArguments: self.linkedTaskArguments[task] = []
            if taskArgument not in self.linkedTaskArguments[task]: self.linkedTaskArguments[task].append(taskArgument)

    # TODO THIS CHECK WAS REMOVED FOR FASTQ-TANGRAM AS TWO ARGUMENTS ARE REQUIRED TO POINT TO THE
    # SAME ARGUMENT. MAYBE ONLY PERFORM THIS CHECK IF THE TOOL ARGUMENT ALLOWS MULTIPLE VALUES.
    # Each node in the pipeline configuration file contains a list of task/argument pairs that take the
    # same value and can thus be merged into a single node in the pipeline graph. If a task/argument pair
    # appears in multiple nodes, the results can be unexpected, so this isn't permitted.
    #for task in observedArguments:
    #  for argument in observedArguments[task]:
    #    if len(observedArguments[task][argument]) > 1:
    #      if self.allowTermination: self.errors.repeatedArgumentInNode(task, argument, observedArguments[task][argument])
    #      else: return False

    return True

  # Get information about any associated extensions. Check that this only occurs for nodes with an
  # filename stub and that all linked arguments that are not stubs themselves are given an extension.
  def checkCommonNodes(self, tools):
    isFilenameStub = False

    # Loop over all of the nodes.
    for configNodeID in self.commonNodes:
      hasFilenameStub = False
      stubArguments   = []
      for task, argument in self.commonNodes[configNodeID]:
        tool = self.taskAttributes[task].tool

        # Check if the argument is 'read json file'. If the argument has this value set, then it
        # isn't referring to a specific argument, but means that the output of one tool in the node
        # is a json file, and the current tool will use the json file to set parameters.
        if argument == 'read json file':

          # Check that one of the arguments in the node outputs a json file.
          hasJsonOutput = False
          for checkTask, checkArgument in self.commonNodes[configNodeID]:
            if checkTask != task and checkArgument != argument:
              checkTool  = self.taskAttributes[checkTask].tool
              isOutput   = tools.getArgumentAttribute(checkTool, checkArgument, 'isOutput')
              extensions = tools.getArgumentAttribute(checkTool, checkArgument, 'extensions')
              for extension in extensions:
                if isOutput and extension.endswith('json'): hasJsonOutput = True

          # If no json files are output as part of this node, the task reading in the json will not get
          # the required information, so fail.
          if not hasJsonOutput: self.errors.noJsonOutput(configNodeID, task)

        # First check if the argument is valid.
        elif argument not in tools.getArguments(tool): self.errors.invalidToolArgument(configNodeID, task, tool, argument, tools.getArguments(tool))
        else: isFilenameStub = tools.getArgumentAttribute(tool, argument, 'isFilenameStub')

        # Check if any of the arguments are for filename stubs.
        if isFilenameStub:
          hasFilenameStub = True
          stubArguments.append((task, argument))

      # If the configuration file node contains an argument with a filename stub, loop over the
      # task/argument pairs again and check that all non-filename stub arguments are provided with
      # a valid extension.
      if hasFilenameStub:
        for task, argument in self.commonNodes[configNodeID]:
	  tool           = self.taskAttributes[task].tool
          isFilenameStub = tools.getArgumentAttribute(tool, argument, 'isFilenameStub')

          # If the argument is also for a filename stub, no further action is necessary. If it is not,
          # then check that the extensions required by the argument is specified. This is necessary
          # as the argument will point to a single file, and the filename stub points to multiple files,
          # so the particular file needs to be specified.
          if not isFilenameStub:

            # If the argument does not have an extension, terminate.
            try: extension = self.nodeAttributes[configNodeID].extensions[task][argument]
            except: self.errors.noExtensionInNode(configNodeID, task, argument, stubArguments)

            # Store the extension.
            self.linkedExtension[configNodeID] = self.nodeAttributes[configNodeID].extensions

  # Set the workflow and the taskAttributes for a tool.
  def definePipelineAttributesForTool(self, name):
    attributes                = taskAttributes()
    attributes.tool           = name
    self.taskAttributes[name] = attributes
    self.workflow.append(name)

  # Set a value in the toolAttributes.
  def setAttribute(self, attributes, attribute, value):
    try: test = getattr(attributes, attribute)

    # If the attribute can't be set, determine the source of the problem and provide an
    # error message.
    except:

      # If the tool is not available.TODO
      self.errors.invalidAttributeInSetAttribute(attribute, True)
      self.errors.terminate()

    # Set the attribute.
    setattr(attributes, attribute, value)

    return attributes

  # Get a task attribute.
  def getTaskAttribute(self, task, attribute):
    try: value = getattr(self.taskAttributes[task], attribute)
    except:

      #TODO ERRORS
      # If the task doesn't exist.
      if task not in self.taskAttributes: print('config.pipeline.getTaskAttribute error', task, attribute); self.errors.terminate()

      # If the attribute is not available.
      if attribute not in self.taskAttributes[task]: print('config.pipeline.getTaskAttribute error attribute', task, attribute); self.errors.terminate()

    return value

  # Get a node attribute.
  def getNodeAttribute(self, nodeID, attribute):
    try: value = getattr(self.nodeAttributes[nodeID], attribute)
    except:
      # TODO ERRORS
      if nodeID not in self.nodeAttributes: print('config.pipeline.getNodeAttribute error', nodeID, attribute); self.errors.terminate()
      if attribute not in self.nodeAttributes[nodeID]:
        print('config.pipeline.getNodeAttribute error attribute', nodeID, attribute); self.errors.terminate()

    return value

  # Get the long form argument for a command given on the command line.
  def getLongFormArgument(self, graph, argument, allowTermination = True):

    # Check if the argument is a pipeline argument (as defined in the configuration file).
    for pipelineArgument in self.pipelineArguments:
      if pipelineArgument == argument: return pipelineArgument, self.pipelineArguments[pipelineArgument].shortFormArgument
      elif self.pipelineArguments[pipelineArgument].shortFormArgument == argument: return pipelineArgument, argument

    if allowTermination: self.errors.unknownPipelineArgument(argument)

    return None, None

  # Check if an argument is a pipeline argument.  If so, return the nodeID.
  def isArgumentAPipelineArgument(self, argument):
    try: return self.pipelineArguments[argument].ID
    except: return None

  # Check if a given a task and argument correspond to a pipeline argument. If so, return the
  # long and short forms.
  def getPipelineArgument(self, task, argument):
    try: longFormArgument = self.taskArgument[task][argument]
    except: return None, None

    if longFormArgument == None: return None, None
    return longFormArgument, self.pipelineArguments[longFormArgument].shortFormArgument

  # Get the extension associated with a task/argument pair from the nodes section.
  def getExtension(self, task, argument, extensions):
    try: return extensions[task][argument]
    except: self.errors.invalidExtensionRequest(task, argument, extensions)

  # Clear a pipeline from storage.
  def clearPipeline(self):
    self.__init__()

  # Get a pipeline attribute.
  def getPipelineAttribute(self, attribute):
    try: value = getattr(self.attributes, attribute)
    except:
      # TODO ERROR
      print('pipeline.getPipelineArgument')
      self.errors.terminate()

    return value
