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

class argumentAttributes:
  def __init__(self):
    self.description = 'No description'
    self.isRequired  = False
    self.nodeID      = ''
    self.shortForm   = ''

class pipelineConfiguration:
  def __init__(self):
    self.argumentData        = {}
    self.configurationData   = {}
    self.description         = 'No description provided'
    self.edgeMethods         = edgeClass()
    self.errors              = configurationClassErrors()
    self.filename            = ''
    self.greedyTasks         = {}
    self.keepFiles           = {}
    self.nodeTaskInformation = {}
    self.nodeMethods         = nodeClass()
    self.pipelineName        = ''
    self.streamingNodes      = {}
    self.tasks               = {}

    # Define a dictionary to store information about extensions.
    self.linkedExtension     = {}

    # Define a structure that links the task and argument described in the 'tasks' list in
    # the nodes section with the pipeline argument.
    self.pipelineArgument = {}

    # Keep track of tasks that output to the stream
    self.tasksOutputtingToStream = {}

  #TODO
  # Validate the contents of the tool configuration file.
  def processConfigurationData(self, data, filename):

    # First validate the contents of the data structure.
    success = self.validateConfigurationData(data)
    if not success: return False

    # Now put all of the data into data structures.
    self.configurationData = data

    # Get and store information on the pipeline arguments.
    self.getPipelineNodeData(data['nodes'])

    return data['instances']

  #TODO
  # Check that the pipeline configuration file is valid.  If so, put all the information in
  # the pipeline data structures.
  def validateConfigurationData(self, data):
    self.description = data['description']

    return True

  #FIXME ERRORS
  # Get information about the pipeline arguments.
  def getPipelineNodeData(self, data):
    self.argumentData        = {}
    self.nodeTaskInformation = {}

    for information in data:
      try: nodeID = information['ID']
      except:
        print('ERROR: PIPELINE CONFIG - ID')
        self.errors.terminate()

      # Get the task/argument information for each of the nodes.
      try: tasks = information['tasks']
      except:
        print('ERROR: NO TASKS INFO - SHOULD BE CAUGHT IN VALIDATE')
        self.errors.terminate()

      self.nodeTaskInformation[nodeID] = []
      for task in tasks: self.nodeTaskInformation[nodeID].append((str(task), str(tasks[task])))

      # Also add information from greedy tasks.
      if 'greedy tasks' in information:
        for task in information['greedy tasks']:
          self.nodeTaskInformation[nodeID].append((str(task), str(information['greedy tasks'][task])))
          self.greedyTasks[task] = str(information['greedy tasks'][task])

      # If 'extension' is in the node, this is used to identify the extension of the file that is
      # being linked. Consider the following case. A tool has an output filename stub 'test' and the
      # tool actually produces the files test.A and test.B. In the pipeline, a tool needs to use the
      # output test.A, but the information in the node only links the input argument with the output
      # filename stub and not which of the outputs in particular. The extension field would have the
      # value 'A' and so the file node for this file can be identified.
      if 'extension' in information:
        self.linkedExtension[nodeID] = information['extension']

      # Now look for information pertaining to pipeline arguments.
      if 'long form argument' in information:
        argument                           = information['long form argument']
        self.argumentData[argument]        = argumentAttributes()
        self.argumentData[argument].nodeID = nodeID

        try: self.argumentData[argument].description = information['description']
        except:
          print('ERROR: PIPELINE CONFIG - DESCRIPTION')
          self.errors.terminate()

        try: self.argumentData[argument].shortForm = information['short form argument']
        except:
          print('ERROR: PIPELINE CONFIG - SHORT FORM')
          self.errors.terminate()

        if 'required' in information: self.argumentData[argument].isRequired = information['required'] 

        # Add the task/argument to the nodeTaskInformation structure as tuples.
        for task in tasks:
          if task not in self.pipelineArgument: self.pipelineArgument[task] = {}
          self.pipelineArgument[task][tasks[task]] = argument

      # Now look to see if the 'keep files' tag is included in the configuration file for this
      # node. This is an indiciation that the file is not an intermediate file.
      self.keepFiles[nodeID] = information['keep files'] if 'keep files' in information else False

      # Now look to see if the 'keep files' tag is included in the configuration file for this
      # node. This is an indiciation that the file is not an intermediate file.
      self.streamingNodes[nodeID] = information['is stream'] if 'is stream' in information else False

  # Parse the pipeline configuration data and return a dictionary contaiing all of the tasks
  # appearing in the pipeline along with the tool required to perform the task.
  def getTasks(self):
    for task in self.configurationData['tasks']:
      tool             = self.configurationData['tasks'][task]['tool']
      self.tasks[task] = tool

      # Check if the tasks output to the stream.
      if 'output to stream' in self.configurationData['tasks'][task]:
        self.tasksOutputtingToStream[task] = True

    # Add the tasks listed as 'greedy tasks'.
    if 'greedy tasks' in self.configurationData:
      for task in self.configurationData['greedy tasks']:
        tool             = self.configurationData['greedy tasks'][task]['tool']
        self.tasks[task] = tool

    tasks = deepcopy(self.tasks)
    return tasks

  # Erase all of the data contained in the self.configurationData structure.
  def eraseConfigurationData(self):
    self.configurationData = {}

  # Get the long form argument for a command given on the command line.
  def getLongFormArgument(self, graph, argument):

    # Check if the argument is a pipeline argument (as defined in the configuration file).
    for pipelineArgument in self.argumentData:
      if pipelineArgument == argument: return pipelineArgument
      elif self.argumentData[pipelineArgument].shortForm == argument: return pipelineArgument

    self.errors.unknownPipelineArgument(argument)

  # Check if an argument is a pipeline argument.  If so, return the nodeID.
  def isArgumentAPipelineArgument(self, argument):
    try: nodeID = self.argumentData[argument].nodeID
    except: return None

    return nodeID

  # Check if a given a task and argument correspond to a pipeline argument. If so, return the
  # long and short forms.
  def getPipelineArgument(self, task, argument):
    try: longForm = self.pipelineArgument[task][argument]
    except: return None, None

    return longForm, self.argumentData[longForm].shortForm
