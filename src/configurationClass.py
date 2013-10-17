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
import os
import sys

class configurationClass:
  def __init__(self):
    self.edgeMethods       = edgeClass()
    self.errors            = configurationClassErrors()
    self.nodeIDs           = {}
    self.nodeMethods       = nodeClass()
    self.fileOperations    = fileOperations()
    self.pipeline          = pipelineConfiguration()
    self.tools             = toolConfiguration()

  # Build a graph for an individual task.  The pipeline is built by merging nodes between
  # different tasks.  This step is performed later.
  def buildTaskGraph(self, graph):

    for task in self.pipeline.tasks:
      tool = self.pipeline.tasks[task]

      # Generate the task node.
      self.nodeMethods.buildTaskNode(graph, self.tools, task, tool)

      # Find all required arguments for this task and build nodes for them all.  Link these nodes
      # to the task node.
      self.nodeMethods.buildRequiredPredecessorNodes(graph, self.tools, task, tool)

      # TODO ENSURE THAT ADDITIONAL FILES, E.G. STUBS AND INDEXES ARE INCLUDED.

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
      forSorting    = {}
      for task, argument in self.pipeline.nodeTaskInformation[nodeName]:
        existingNodes[str(task) + str(argument)] = (task, argument, self.nodeMethods.doesNodeExist(graph, task, argument))
        forSorting[str(task) + str(argument)]    = self.nodeMethods.doesNodeExist(graph, task, argument)

      # Sort the nodes by the nodeID. All values that are None will appear at the beginning.
      #sortedExistingNodes = sorted(existingNodes.iteritems(), key=operator.itemgetter(1))
      sortedNodes = sorted(forSorting.iteritems(), key=operator.itemgetter(1))
      sortedExistingNodes = []
      for ID in sortedNodes: sortedExistingNodes.append(existingNodes[ID[0]])

      # The last item is the node to be kept.  If its value is None, none of the nodes exist and
      # a node with all connections needs to be created.
      lastNode = sortedExistingNodes.pop(len(sortedExistingNodes) - 1)
      task       = lastNode[0]
      argument   = lastNode[1]
      keptNodeID = lastNode[2]

      if keptNodeID == None:
        tool       = self.pipeline.tasks[task]
        attributes = self.nodeMethods.buildNodeFromToolConfiguration(self.tools, tool, argument)

        # Check if the argument is required or not.  Only required nodes are built here.
        keptNodeID = str('OPTION_') + str(self.nodeMethods.optionNodeID)
        self.nodeMethods.optionNodeID += 1
        graph.add_node(keptNodeID, attributes = attributes)

      # Store the ID of the node being kept along with the value it was given in the common nodes
      # section of the configuration file.  The instances information will refer to the common
      # node value and this needs to point to the nodeID in the graph.
      self.nodeIDs[nodeName] = keptNodeID

      # Loop over the remaining nodes, delete them and include edges to the retained nodes.
      #for ID, values in sortedExistingNodes:
      for values in sortedExistingNodes:
        task       = values[0]
        argument   = values[1]
        nodeID     = values[2]
        tool       = self.pipeline.tasks[task]
        if nodeID != None: shortForm  = self.edgeMethods.getEdgeAttribute(graph, nodeID, task, 'shortForm')
        else: shortForm = self.tools.getArgumentData(tool, argument, 'short form argument')

        # Add an edge from the common node to this task.
        attributes           = edgeAttributes()
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

  # Generate the task workflow from the topologically sorted pipeline graph.
  def generateWorkflow(self, graph):
    workflow  = []
    topolSort = nx.topological_sort(graph)
    for nodeID in topolSort:
      nodeType = self.nodeMethods.getGraphNodeAttribute(graph, nodeID, 'nodeType')
      if nodeType == 'task': workflow.append(nodeID)

    return workflow

  # Check that all defined parameters are valid.
  def checkParameters(self, graph):
    print('***NEED TO CHECK PARAMETERS')
