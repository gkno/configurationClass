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
    self.description = None

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
    self.keepFiles         = False
    self.ID                = None
    self.greedyTasks       = None
    self.isRequired        = False
    self.longFormArgument  = None
    self.shortFormArgument = None
    self.tasks             = None

# Define a class to store pipeline argument attributes.
class argumentAttributes:
  def __init__(self):
    self.description       = None
    self.isRequired        = False
    self.ID                = None
    self.configNodeID      = None
    self.shortFormArgument = None

class pipelineConfiguration:
  def __init__(self):

    # Define the attributes for a pipeline.
    self.attributes = {}

    # Define the task attributes.
    self.taskAttributes = {}

    # Define the node attributes.
    self.nodeAttributes = {}

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
    self.linkedExtension     = {}

    # Define a structure that links the task and argument described in the 'tasks' list in
    # the nodes section with the pipeline argument.
    self.taskArgument = {}

    # Keep track of tasks that output to the stream
    self.tasksOutputtingToStream = {}

    # Define the methods to operate on the graph nodes and edges,
    self.edgeMethods = edgeClass()
    self.nodeMethods = nodeClass()

    #TODO REQUIRED?
    self.filename            = ''
    self.pipelineName        = ''

  #TODO
  # Validate the contents of the tool configuration file.
  def processConfigurationData(self, data, pipeline, toolFiles):

    # Parse the pipeline configuration file and check that all fields are valid. Ensure that there are
    # no errors, omissions or inconsistencies. Store the information in the relevant data structures
    # as the checks are performed.
    #
    # Check the general tool information.
    self.attributes = self.checkGeneralAttributes(pipeline, data)

    # Check the 'tasks' section of the configuration file.
    self.checkTasks(pipeline, data['tasks'], toolFiles)

    # Check the contents of the nodes section.
    self.checkNodes(pipeline, data['nodes'])

    # Check and store the pipeline arguments.
    self.setPipelineArguments()

    # From the node data, define which arguments are greedy.
    self.getNodeTasks(pipeline)

  def checkGeneralAttributes(self, pipeline, data):

    # Set the general tool attributes.
    attributes = pipelineAttributes()

    # Define the allowed general attributes.
    allowedAttributes                = {}
    allowedAttributes['description'] = (str, True, True, 'description')
    allowedAttributes['instances']   = (list, True, False, None)
    allowedAttributes['nodes']       = (list, True, False, None)
    allowedAttributes['tasks']       = (dict, True, False, None)

    # Keep track of the observed required values.
    observedAttributes = {}

    # Loop over all of the attributes in the configuration file.
    for attribute in data:

      # If the value is not in the allowedAttributes, it is not an allowed value and execution
      # should be terminate with an error.
      if attribute not in allowedAttributes: self.errors.invalidGeneralAttributeInConfigurationFile(pipeline, attribute, allowedAttributes, True)

      # Mark this values as having been observed,
      observedAttributes[attribute] = True

      # Check that the value given to the attribute is of the correct type. If the value is unicode,
      # convert to a string first.
      value = str(data[attribute]) if isinstance(data[attribute], unicode) else data[attribute]
      if allowedAttributes[attribute][0] != type(value):
        self.errors.incorrectTypeInPipelineConfigurationFile(pipeline, attribute, value, allowedAttributes[attribute][0], 'general')

      # At this point, the attribute in the configuration file is allowed and of valid type. Check that 
      # the value itself is valid (if necessary) and store the value.
      if allowedAttributes[attribute][2]: self.setAttribute(attributes, allowedAttributes[attribute][3], value)

    # Having parsed all of the general attributes attributes, check that all those that are required
    # are present.
    for attribute in allowedAttributes:
      if allowedAttributes[attribute][1] and attribute not in observedAttributes:
        self.errors.missingGeneralAttributeInConfigurationFile(pipeline, attribute, allowedAttributes, True)

    return attributes

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
      if not isinstance(tasks[task], dict): self.errors.taskIsNotDictionary(pipeline, task)

      # Loop over the included attributes.
      for attribute in tasks[task]:
        if attribute not in allowedAttributes: self.errors.invalidAttributeInTasks(pipeline, task, attribute, allowedAttributes)

        # Check that the value given to the attribute is of the correct type. If the value is unicode,
        # convert to a string first.
        value = str(tasks[task][attribute]) if isinstance(tasks[task][attribute], unicode) else tasks[task][attribute]
        if allowedAttributes[attribute][0] != type(value):
          self.errors.incorrectTypeInPipelineConfigurationFile(pipeline, attribute, value, allowedAttributes[attribute][0], 'tasks')

        # Mark the attribute as seen.
        observedAttributes[attribute] = True

        # Store the given attribtue.
        if allowedAttributes[attribute][2]: self.setAttribute(attributes, allowedAttributes[attribute][3], tasks[task][attribute])

      # Having parsed all of the general attributes attributes, check that all those that are required
      # are present.
      for attribute in allowedAttributes:
        if allowedAttributes[attribute][1] and attribute not in observedAttributes:
          self.errors.missingAttributeInPipelineConfigurationFile(pipeline, attribute, allowedAttributes, 'tasks', None)

      # Check that each task has a tool defined and that a tool configuration file exists for this tool.
      tool = tasks[task]['tool']
      if tool + '.json' not in toolFiles: self.errors.invalidToolInPipelineConfigurationFile(pipeline, task, tool)

      # Store the attributes for the task.
      self.taskAttributes[task] = attributes

  # Check the contents of the nodes section.
  def checkNodes(self, pipeline, nodes):

    # Define the allowed nodes attributes.
    allowedAttributes                        = {}
    allowedAttributes['ID']                  = (str, True, True, 'ID')
    allowedAttributes['description']         = (str, True, True, 'description')
    allowedAttributes['extensions']          = (dict, False, True, 'extensions')
    allowedAttributes['greedy tasks']        = (dict, False, True, 'greedyTasks')
    allowedAttributes['keep files']          = (bool, False, True, 'keepFiles')
    allowedAttributes['long form argument']  = (str, False, True, 'longFormArgument')
    allowedAttributes['required']            = (bool, False, True, 'isRequired')
    allowedAttributes['short form argument'] = (str, False, True, 'shortFormArgument')
    allowedAttributes['tasks']               = (dict, True, True, 'tasks')

    # Loop over all of the defined nodes.
    for node in nodes:

      # Check that node is a dictionary.
      if not isinstance(node, dict): self.errors.nodeIsNotADictionary(pipeline)

      # Define the attributes object.
      attributes = pipelineNodeAttributes()

      # Keep track of the observed required values.
      observedAttributes = {}

      # Check that the node has an ID. This will be used to identify the node in error messages.
      try: ID = node['ID']
      except: self.errors.noIDInPipelineNode(pipeline)

      # Loop over all attributes in the node.
      for attribute in node:
        if attribute not in allowedAttributes: self.errors.invalidAttributeInNodes(pipeline, ID, attribute, allowedAttributes)

        # Check that the value given to the attribute is of the correct type. If the value is unicode,
        # convert to a string first.
        value = str(node[attribute]) if isinstance(node[attribute], unicode) else node[attribute]
        if allowedAttributes[attribute][0] != type(value):
          self.errors.incorrectTypeInPipelineConfigurationFile(pipeline, attribute, value, allowedAttributes[attribute][0], 'nodes')

        # Mark the attribute as seen.
        observedAttributes[attribute] = True

        # Store the given attribtue.
        if allowedAttributes[attribute][2]: self.setAttribute(attributes, allowedAttributes[attribute][3], node[attribute])

      # Having parsed all of the general attributes attributes, check that all those that are required
      # are present.
      for attribute in allowedAttributes:
        if allowedAttributes[attribute][1] and attribute not in observedAttributes:
          self.errors.missingAttributeInPipelineConfigurationFile(pipeline, attribute, allowedAttributes, 'nodes', ID)

      # Store the attributes.
      self.nodeAttributes[ID] = attributes

  # Check the validity and completeness of the pipeline argument definitions.
  def setPipelineArguments(self):

    # Loop over all of the nodes and set the pipeline arguments.
    for nodeID in self.nodeAttributes:

      # The long form argument will be used as the key in this dictionary.
      longFormArgument = self.nodeAttributes[nodeID].longFormArgument

      # Set the other attributes only if the long form argument is present.
      if longFormArgument:

        # Define the structure to hold the argument information for this pipeline.
        attributes = argumentAttributes()

        self.setAttribute(attributes, 'description', self.nodeAttributes[nodeID].description)
        self.setAttribute(attributes, 'configNodeID', nodeID)
        self.setAttribute(attributes, 'shortFormArgument', self.nodeAttributes[nodeID].shortFormArgument)
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

      # Parse the tasks.
      if self.nodeAttributes[configNodeID].tasks:
        for task in self.nodeAttributes[configNodeID].tasks:

          # Check that the task is valid.
          if task not in self.taskAttributes.keys(): self.errors.invalidTaskInNode(pipeline, configNodeID, task, False)

          # Link the pipeline argument to the task/arguments listed with the node.
          taskArgument = self.nodeAttributes[configNodeID].tasks[task]
          if task not in self.taskArgument: self.taskArgument[task] = {}
          self.taskArgument[task][taskArgument] = self.nodeAttributes[configNodeID].longFormArgument

          # Store the task and argument.
          self.commonNodes[configNodeID].append((str(task), str(taskArgument)))

          # Store the task/argument pair in the observedOptions dictionary. If this task/argument pair has already been seen
          if str(task) not in observedArguments: observedArguments[str(task)] = {}
          if str(taskArgument) not in observedArguments[str(task)]: observedArguments[str(task)][str(taskArgument)] = []
          observedArguments[str(task)][str(taskArgument)].append(str(configNodeID))

      # Then parse the greedy tasks.
      if self.nodeAttributes[configNodeID].greedyTasks:
        for task in self.nodeAttributes[configNodeID].greedyTasks:

          # Check that the task is valid.
          if task not in self.taskAttributes.keys(): self.errors.invalidTaskInNode(pipeline, configNodeID, task, True)

          # Link the pipeline argument to the task/arguments listed with the node.
          if task not in self.taskArgument: self.taskArgument[task] = {}
          taskArgument = self.nodeAttributes[configNodeID].greedyTasks[task]
          self.taskArgument[task][taskArgument] = self.nodeAttributes[configNodeID].longFormArgument

          # Store the task and argument.
          self.commonNodes[configNodeID].append((str(task), str(taskArgument)))
          self.greedyTasks[task] = str(self.nodeAttributes[configNodeID].greedyTasks)

          # Store the task/argument pair in the observedOptions dictionary. If this task/argument pair has already been seen
          if str(task) not in observedArguments: observedArguments[str(task)] = {}
          if str(taskArgument) not in observedArguments[str(task)]: observedArguments[str(task)][str(taskArgument)] = []
          observedArguments[str(task)][str(taskArgument)].append(str(configNodeID))

    # Each node in the pipeline configuration file contains a list of task/argument pairs that take the
    # same value and can thus be merged into a single node in the pipeline graph. If a task/argument pair
    # appears in multiple nodes, the results can be unexpected, so this isn't permitted.
    for task in observedArguments:
      for argument in observedArguments[task]:
        if len(observedArguments[task][argument]) > 1: self.errors.repeatedArgumentInNode(task, argument, observedArguments[task][argument])

  # Get information about any associated extensions. Check that this only occurs for nodes with an
  # filename stub and that all linked arguments that are not stubs themselves are given an extension.
  def checkCommonNodes(self, tools):

    # Loop over all of the nodes.
    for configNodeID in self.commonNodes:
      hasFilenameStub = False
      stubArguments   = []
      for task, argument in self.commonNodes[configNodeID]:
        tool = self.taskAttributes[task].tool

        # First check if the argument is valid.
        if argument not in tools.getArguments(tool): self.errors.invalidToolArgument(configNodeID, task, tool, argument, tools.getArguments(tool))

        # Check if any of the arguments are for filename stubs.
        isFilenameStub = tools.getArgumentAttribute(tool, argument, 'isFilenameStub')
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

  # Get the long form argument for a command given on the command line.
  def getLongFormArgument(self, graph, argument):

    # Check if the argument is a pipeline argument (as defined in the configuration file).
    for pipelineArgument in self.pipelineArguments:
      if pipelineArgument == argument: return pipelineArgument
      elif self.pipelineArguments[pipelineArgument].shortFormArgument == argument: return pipelineArgument

    self.errors.unknownPipelineArgument(argument)

  # Check if an argument is a pipeline argument.  If so, return the nodeID.
  def isArgumentAPipelineArgument(self, argument):
    try: nodeID = self.pipelineArguments[argument].ID
    except: return None

    return nodeID

  # Check if a given a task and argument correspond to a pipeline argument. If so, return the
  # long and short forms.
  def getPipelineArgument(self, task, argument):
    try: longFormArgument = self.taskArgument[task][argument]
    except: return None, None

    return longFormArgument, self.pipelineArguments[longFormArgument].shortFormArgument

  # Get the extension associated with a task/argument pair from the nodes section.
  def getExtension(self, task, argument, extensions):
    try: return extensions[task][argument]
    except: self.errors.invalidExtensionRequest(task, argument, extensions)
