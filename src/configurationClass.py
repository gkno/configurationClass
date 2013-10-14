#!/bin/bash/python

from __future__ import print_function
import networkx as nx
from copy import deepcopy

import configurationClassErrors
from configurationClassErrors import *

import edgeAttributes
from edgeAttributes import *

import fileOperations
from fileOperations import *

import nodeAttributes
from nodeAttributes import *

import pipelineAttributes
from pipelineAttributes import *

import toolAttributes
from toolAttributes import *

import json
import operator
import os
import sys

class configurationClass:
  def __init__(self):
    self.edgeMethods       = edgeClass()
    self.errors            = configurationClassErrors()
    self.nodeIDs           = {}
    self.nodeMethods       = nodeClass()
    self.fileOperations    = fileOperations()
    self.optionNodeID      = 1
    self.pipeline          = pipelineConfiguration()
    self.tools             = toolConfiguration()

  # Build a graph for an individual task.  The pipeline is built by merging nodes between
  # different tasks.  This step is performed later.
  def buildTaskGraph(self, graph):

    for task in self.pipeline.tasks:

      # Generate the task node.
      self.buildTaskNode(graph, task)

      # Find all required arguments for this task and build nodes for them all.  Link these nodes
      # to the task node.
      self.buildRequiredPredecessorNodes(graph, task)

      # TODO ENSURE THAT ADDITIONAL FILES, E.G. STUBS AND INDEXES ARE INCLUDED.

  # TODO FINISH THIS
  # Build a task node.
  def buildTaskNode(self, graph, task):
    attributes             = taskNodeAttributes()
    attributes.tool        = self.pipeline.tasks[task]
    attributes.description = self.tools.getConfigurationData(attributes.tool, 'description')
    graph.add_node(task, attributes = attributes)

  # Build all of the predecessor nodes for the task and attach them to the task node.
  def buildRequiredPredecessorNodes(self, graph, task):
    tool = self.pipeline.tasks[task]
    for argument in self.tools.configurationData[tool]['arguments']:
      attributes = self.tools.buildNodeFromToolConfiguration(tool, argument)

      # Check if the argument is required or not.  Only required nodes are built here.
      isRequired = self.nodeMethods.getNodeAttribute(attributes, 'isRequired')
      if isRequired:
        nodeID = str('OPTION_') + str(self.optionNodeID)
        self.optionNodeID += 1
        graph.add_node(nodeID, attributes = attributes)

        # Add an edge to the task node.
        edge          = edgeAttributes()
        edge.argument = argument
        graph.add_edge(nodeID, task, attributes = edge)
 
        # If the node represents an option for defining an input or output file, create
        # a file node.
        shortForm = self.tools.getArgumentData(tool, argument, 'short form argument')
        if self.nodeMethods.getGraphNodeAttribute(graph, nodeID, 'isInput'):
          self.buildTaskFileNodes(graph, nodeID, task, argument, shortForm, 'input')
        elif self.nodeMethods.getGraphNodeAttribute(graph, nodeID, 'isOutput'):
          self.buildTaskFileNodes(graph, nodeID, task, argument, shortForm, 'output')

  # Add input file nodes.
  def buildTaskFileNodes(self, graph, nodeID, task, argument, shortForm, fileType):

    # TODO DEAL WITH MULTIPLE FILE NODES.
    attributes                     = fileNodeAttributes()
    attributes.description         = self.nodeMethods.getGraphNodeAttribute(graph, nodeID, 'description')
    attributes.allowMultipleValues = self.nodeMethods.getGraphNodeAttribute(graph, nodeID, 'allowMultipleValues')
    attributes.allowedExtensions   = self.nodeMethods.getGraphNodeAttribute(graph, nodeID, 'allowedExtensions')
    fileNodeID                     = nodeID + '_FILE'
    graph.add_node(fileNodeID, attributes = attributes)

    # Add the edge.
    edge           = edgeAttributes()
    edge.argument  = argument
    edge.shortForm = shortForm
    if fileType == 'input': graph.add_edge(fileNodeID, task, attributes = edge)
    elif fileType == 'output': graph.add_edge(task, fileNodeID, attributes = edge)

    # Add the file node to the list of file nodes associated with the option node.
    self.nodeMethods.setGraphNodeAttribute(graph, nodeID, 'associatedFileNodes', fileNodeID)

  # Generate the task workflow from the topologically sorted pipeline graph.
  def generateWorkflow(self, graph):
    workflow  = []
    topolSort = nx.topological_sort(graph)
    for nodeID in topolSort:
      nodeType = self.nodeMethods.getGraphNodeAttribute(graph, nodeID, 'nodeType')
      if nodeType == 'task': workflow.append(nodeID)

    return workflow

  # Check each option node and determine if a value is required.  This can be determined in one of two
  # ways.  If any of the edges beginning at the option node correspond to a tool argument that is
  # listed as required by the tool, or if the node corresponds to a command line argument that is
  # listed as required.  If the node is a required pipeline argument, it has already been tagged as
  # required.
  def setRequiredNodes(self, graph):

    # Loop over all data nodes.
    for nodeID in graph.nodes(data = False):
      nodeType = self.nodeMethods.getGraphNodeAttribute(graph, nodeID, 'nodeType')
      if nodeType == 'option':
        for edge in graph.edges(nodeID):
          task           = edge[1]
          associatedTool = self.nodeMethods.getGraphNodeAttribute(graph, task, 'tool')
          toolArgument   = self.edgeMethods.getEdgeAttribute(graph, nodeID, task, 'argument')
          isRequired     = self.tools.getArgumentData(associatedTool, toolArgument, 'required')
          if isRequired: self.nodeMethods.setGraphNodeAttribute(graph, nodeID, 'isRequired', True)
          break

  # Merge shared nodes between tasks using information from the pipeline configuration file.  For
  # example, task A outputs a file fileA and taskB uses this as input.  Having built an individual
  # graph for each task, there exists an output file node for taskA and an input file node for
  # taskB (and also option nodes defining the names), but these are the same file and so these
  # nodes can be merged into a single node.
  def mergeNodes(self, graph):
    for nodeName in self.pipeline.nodeTaskInformation:

      # The configuration file lists all the tasks (and arguments) that use the node.  The nodes
      # themselves may have already been placed in the graph, or may not be present or are present
      # for some of the tasks but not others (for example, it may be that only nodes listed as
      # required appear in the graph).  For each task/argument pair, determine if the node exists.
      existingNodes = {}
      for task, argument in self.pipeline.nodeTaskInformation[nodeName]:
        existingNodes[str(task) + str(argument)] = (task, argument, self.doesNodeExist(graph, task, argument))

      # Sort the common nodes by the value. All values that are None will appear at the beginning.
      sortedExistingNodes = sorted(existingNodes.iteritems(), key=operator.itemgetter(1))

      # The first item is the node to be kept.  If its value is None, none of the nodes exist and
      # a node with all connections needs to be created.
      #firstNode = sortedExistingNodes.pop(0)
      lastNode   = sortedExistingNodes.pop(len(sortedExistingNodes) - 1)[1]
      task       = lastNode[0]
      argument   = lastNode[1]
      keptNodeID = lastNode[2]

      if keptNodeID == None:
        tool       = self.pipeline.tasks[task]
        attributes = self.tools.buildNodeFromToolConfiguration(tool, argument)

        # Check if the argument is required or not.  Only required nodes are built here.
        keptNodeID                 = str('OPTION_') + str(self.optionNodeID)
        self.optionNodeID += 1
        graph.add_node(keptNodeID, attributes = attributes)

      # Store the ID of the node being kept along with the value it was given in the common nodes
      # section of the configuration file.  The instances information will refer to the common
      # node value and this needs to point to the nodeID in the graph.
      self.nodeIDs[nodeName] = keptNodeID

      # Loop over the remaining nodes, delete them and include edges to the retained nodes.
      for ID, values in sortedExistingNodes:
        task       = values[0]
        argument   = values[1]
        nodeID     = values[2]
        tool       = self.pipeline.tasks[task]
        if nodeID != None: shortForm  = self.edgeMethods.getEdgeAttribute(graph, nodeID, task, 'shortForm')
        else: shortForm = self.tools.getArgumentData(tool, argument, 'short form argument')

        # Add an edge from the common node to this task.
        attributes = edgeAttributes()
        attributes.argument  = argument
        attributes.shortForm = shortForm
        graph.add_edge(keptNodeID, task, attributes = attributes)

        # If this option node has associated file nodes, delete them and add edges to the file
        # nodes associated with the kept node.
        isInput  = self.tools.getArgumentData(tool, argument, 'input')
        isOutput = self.tools.getArgumentData(tool, argument, 'output')
        if nodeID != None: 
          for fileNodeID in self.nodeMethods.getGraphNodeAttribute(graph, nodeID, 'associatedFileNodes'):
            graph.remove_node(fileNodeID)

        for fileNodeID in self.nodeMethods.getGraphNodeAttribute(graph, keptNodeID, 'associatedFileNodes'):
          if isInput: graph.add_edge(fileNodeID, task, attributes = attributes)
          elif isOutput: graph.add_edge(task, fileNodeID, attributes = attributes)

        # Remove the node.  First check if this node is associated with a pipeline argument.  If so, reassign
        # the pipeline argument to the keptNodeID.
        if nodeID != None:
          for pipelineArgument in self.pipeline.argumentData:
            pipelineArgumentNodeID = self.pipeline.argumentData[pipelineArgument].nodeID
            if pipelineArgumentNodeID == nodeID: self.pipeline.argumentData[pipelineArgument].nodeID = keptNodeID
          graph.remove_node(nodeID)

  # Check if a node exists based on a task and an argument.
  def doesNodeExist(self, graph, task, argument):
    exists = False
    for sourceNode, targetNode in graph.in_edges(task):
      edgeArgument = self.edgeMethods.getEdgeAttribute(graph, sourceNode, targetNode, 'argument')
      nodeType     = self.nodeMethods.getGraphNodeAttribute(graph, sourceNode, 'nodeType')
      if nodeType == 'option' and edgeArgument == argument:
        exists = True
        return sourceNode

    return None

  # Get all of the input or output nodes for a task.
  def getInputOutputNodes(self, graph, task, isInput):
    nodes = []

    # Loop over all of the predecessor nodes and then the successor nodes.
    for node in graph.predecessors(task):
      print('pre', node)
      if self.nodeMethods.getGraphNodeAttribute(graph, node, 'nodeType') == 'file':
        if isInput and self.nodeMethods.getGraphNodeAttribute(graph, node, 'isInput'): nodes.append(node)
        elif not isInput and self.nodeMethods.getGraphNodeAttribute(graph, node, 'isOutput'): nodes.append(node)

    for node in graph.successors(task):
      if self.nodeMethods.getGraphNodeAttribute(graph, node, 'nodeType') == 'file':
        print('suc', node, isInput, self.nodeMethods.getGraphNodeAttribute(graph, node, 'isOutput'))
        if isInput and self.nodeMethods.getGraphNodeAttribute(graph, node, 'isInput'): nodes.append(node)
        elif not isInput and self.nodeMethods.getGraphNodeAttribute(graph, node, 'isOutput'): nodes.append(node)

    return nodes
