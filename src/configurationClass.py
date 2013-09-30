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
    self.nodeMethods       = nodeClass()
    self.fileOperations    = fileOperations()
    self.optionNodeID      = 1
    self.pipeline          = pipelineConfiguration()
    self.tools             = toolConfiguration()

  # Build a graph for an individual task.  The pipeline is built by merging nodes between
  # different tasks.  This step is performed later.
  def buildTaskGraph(self, graph, task, tool, pipeData, toolData):

    # Generate the task node.
    self.buildTaskNode(graph, task, pipeData)

    # Find all required arguments for this task and build nodes for them all.  Link these nodes
    # to the task node.
    self.buildRequiredPredecessorNodes(graph, task, tool, toolData['arguments'])

    # TODO ENSURE THAT ADDITIONAL FILES, E.G. STUBS AND INDEXES ARE INCLUDED.

  # TODO FINISH THIS
  # Build a task node.
  def buildTaskNode(self, graph, task, pipeData):
    attributes      = taskNodeAttributes()
    attributes.tool = pipeData['tool']
    graph.add_node(task, attributes = attributes)

  # Build all of the predecessor nodes for the task and attach them to the task node.
  def buildRequiredPredecessorNodes(self, graph, task, tool, data):
    for argument in data:
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
        shortForm = data[argument]['short form argument'] if 'short form argument' in data[argument] else ''
        if self.nodeMethods.getGraphNodeAttribute(graph, nodeID, 'isInput'):
          self.buildTaskFileNodes(graph, nodeID, task, argument, shortForm, 'input')
        elif self.nodeMethods.getGraphNodeAttribute(graph, nodeID, 'isOutput'):
          self.buildTaskFileNodes(graph, nodeID, task, argument, shortForm, 'output')

  # Add input file nodes.
  def buildTaskFileNodes(self, graph, nodeID, task, argument, shortForm, fileType):
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
    if fileType == 'input': graph.add_edge(nodeID, task, attributes = edge)
    elif fileType == 'output': graph.add_edge(task, nodeID, attributes = edge)

  # Merge shared nodes between tasks using information from the pipeline configuration file.  For
  # example, task A outputs a file fileA and taskB uses this as input.  Having built an individual
  # graph for each task, there exists an output file node for taskA and an input file node for
  # taskB (and also option nodes defining the names), but these are the same file and so these
  # nodes can be merged into a single node.
  def mergeNodes(self, graph, commonNodes):
    for commonNodeID in commonNodes:

      # The configuration file lists all the tasks (and arguments) that use the node.  The nodes
      # themselves may have already been placed in the graph, or may not be present or are present
      # for some of the tasks but not others (for example, it may be that only nodes listed as
      # required appear in the graph).  For each task/argument pair, determine if the node exists.
      existingNodes = {}
      for task in commonNodes[commonNodeID]:
        argument = commonNodes[commonNodeID][task]
        existingNodes[str(task) + str(argument)] = self.doesNodeExist(graph, task, argument)

      # For each of the common nodes, determine if the node exists for all, some or none of the
      # tasks for which it is linked.
      missingNodes = 0
      for nodeID in existingNodes:
        if existingNodes[nodeID] == None: missingNodes += 1

      # TODO
      # If the nodes referred to do not exist (this is possible, for example, if the nodes are
      # for non-required arguments), create the node.
      someNodesDefined = False if missingNodes == len(existingNodes) else True
      if not someNodesDefined:
        firstNode = True
        for taskNodeID in commonNodes[commonNodeID]:
          argument = commonNodes[commonNodeID][taskNodeID]
          if firstNode:
            firstNode = False

            # Find the tool for this task and argument.
            tool       = graph.node[taskNodeID]['attributes'].tool

            # Generate the node attributes for this node.
            attributes = self.tools.buildNodeFromToolConfiguration(tool, argument)
            nodeID     = str('OPTION_') + str(self.optionNodeID)
            self.optionNodeID += 1

            # Add this node to the graph
            graph.add_node(nodeID, attributes = attributes)
            
          # Add an edge from the created node to the task.
          attributes           = edgeAttributes()
          attributes.argument  = argument
          shortForm            = self.tools.attributes[tool].arguments
          #attributes.shortForm = graph[existingNodes[existingNodeID]][nodeID]['attributes'].shortForm
          print('ADDING EDGE', nodeID, taskNodeID, argument, shortForm)
          graph.add_edge(nodeID, taskNodeID, attributes = attributes)

        exit(0)

      # If any the nodes are defined, remove all but one of them and insert edges from the
      # one remaining node to the tasks whose nodes were removed.  The node to be kept is
      # arbitrary.
      else:
        nodeToKeep  = True
        connectNode = {}
        for nodeID in commonNodes[commonNodeID]:
          existingNodeID = str(nodeID) + str(commonNodes[commonNodeID][nodeID])
          nodeExists     = False if existingNodes[existingNodeID] == None else True
          if nodeToKeep and nodeExists:
            nodeToKeep = False
            keptNodeID = existingNodes[existingNodeID]
          else:

            # If the node exists in the graph, remove it.
            shortForm = ''
            if existingNodes[existingNodeID] != None:
              shortForm = graph[existingNodes[existingNodeID]][nodeID]['attributes'].shortForm
              graph.remove_node(existingNodes[existingNodeID])

            # Add the argument and the shortForm into the connectNode dictionary.
            connectNode[nodeID] = (commonNodes[commonNodeID][nodeID], shortForm)

        # Generate edges from the kept node to the tasks whose nodes were deleted.
        for nodeID in connectNode:
          attributes           = edgeAttributes()
          attributes.argument  = connectNode[nodeID][0]
          attributes.shortForm = connectNode[nodeID][1]
          graph.add_edge(keptNodeID, nodeID, attributes = attributes)

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



  # Transfer all of the information from the configuration file into data structures.
  def addNodesAndEdges(self, graph, data):
    inputNodes      = {}
    outputNodes     = {}

    # Set the pipeline arguments.
    for argument in data['arguments']:

      # Each new node ID must be unique.  Throw an error if this node ID has been seen before.
      nodeID = data['arguments'][argument]['ID']
      if graph.has_node(nodeID):
        print('non-unique argument node: ', nodeID)
        exit(1)

      # Determine if this argument is for a file or an option.  If it is for a file, attach the
      # fileNodeAttributes structure to the node.
      if data['arguments'][argument]['file']: attributes = fileNodeAttributes()

      # If the argument defines an option, attach the optionNodeAttributes data structure to the
      #node.
      else: attributes = optionNodeAttributes()

      # Add values common to both file and option node data structures.
      self.nodeMethods.setNodeAttribute(attributes, 'argument', argument)
      self.nodeMethods.setNodeAttribute(attributes, 'description', data['arguments'][argument]['description'])
      self.nodeMethods.setNodeAttribute(attributes, 'isPipelineArgument', True)
      self.nodeMethods.setNodeAttribute(attributes, 'shortForm', data['arguments'][argument]['short form argument'])
      if 'required' in data['arguments'][argument]:
        self.nodeMethods.setNodeAttribute(attributes, 'isRequired', data['arguments'][argument]['required'])

      # Add the node to the graph.
      graph.add_node(nodeID, attributes = attributes)

    # Loop through all of the tasks and store all the information about the edges.
    for task in data['tasks']:
      inputNodes[task]  = []
      outputNodes[task] = []

      # Each new node ID must be unique.  Throw an error if this node ID has been seen before.
      if graph.has_node(task):
        print('non-unique task node: ', task)
        exit(1)

      # Create the new node and attach the relevant information to it.
      attributes      = taskNodeAttributes()
      attributes.tool = data['tasks'][task]['tool']
      graph.add_node(task, attributes = attributes)

      # Put all of the input and output nodes into a list, then add all of them to the
      # graph.
      for inputNode in data['tasks'][task]['input nodes']: inputNodes[task].append(inputNode)
      for outputNode in data['tasks'][task]['output nodes']: outputNodes[task].append(outputNode)

    self.addInputOutputNodes(graph, data, inputNodes, 'input nodes')
    self.addInputOutputNodes(graph, data, outputNodes, 'output nodes')

  # Add the nodes listed in the 'input nodes' and 'output nodes' section of the pipeline
  # configuration file to the graph.
  def addInputOutputNodes(self, graph, data, nodes, nodesListID):
    for task in nodes:
      for node in nodes[task]:
        print('hello', task, node, nodesListID)

        # Determine the tool that is used to execute this task.  When parsing the input nodes, we
        # will need to identify the data associated with the node (e.g. a file or an option) in
        # order to add a node with the correct attributes data structure.
        associatedTool = self.nodeMethods.getGraphNodeAttribute(graph, task, 'tool')

        # If the input node is not already in the graph, add it.
        if not graph.has_node(node):

          # Identify the argument associated with this node.
          if nodesListID == 'input nodes': argument = data['tasks'][task]['input nodes'][node]
          else: argument = 'dummy'

          # All dummy arguments are assumed to be file nodes.
          if argument == 'dummy':
            nodeAttributes = fileNodeAttributes()
            graph.add_node(node, attributes = nodeAttributes)
            self.nodeMethods.setGraphNodeAttribute(graph, node, 'isOutput', True)
          else:
            nodeAttributes = self.tools.attributes[associatedTool].arguments[argument]
            graph.add_node(node, attributes = nodeAttributes)

        # Add an edge from the input node to the task.
        edge = edgeAttributes()
        if nodesListID == 'input nodes':
          edge.argument = data['tasks'][task][nodesListID][node]
          graph.add_edge(node, task, attributes = edge)
        else:
          edge.argument = 'dummy'
          graph.add_edge(task, node, attributes = edge)

  # Generate the task workflow from the topologically sorted pipeline graph.
  def generateWorkflow(self, graph):
    workflow  = []
    topolSort = nx.topological_sort(graph)
    for node in topolSort:
      nodeType = self.nodeMethods.getGraphNodeAttribute(graph, node, 'nodeType')
      if nodeType == 'task': workflow.append(node)

    return workflow

  # Set all task node attributes.
  def getRequiredTools(self, graph):
    tools = []
    for node in graph.nodes(data = False):

      # Find the tool used by this task.
      nodeType = self.nodeMethods.getGraphNodeAttribute(graph, node, 'nodeType')
      if nodeType == 'task':
        tool = self.nodeMethods.getGraphNodeAttribute(graph, node, 'tool')
        tools.append(tool)

    return tools

  # Set all task successor nodes as output files.
  def setSuccessorsAsOutputs(self, graph, workflow):
    for task in workflow:
      for node in graph.successors(task):
        self.nodeMethods.setGraphNodeAttribute(graph, node, 'isOutput', True)

  # Check each data node and determine if a value is required.  This can be determined in one of two
  # ways.  If any of the edges beginning at the data node correspond to a tool argument that is
  # listed as required by the tool, or if the node corresponds to a command line argument that is
  # listed as required.  If the node is a required pipeline argument, it has already been tagged as
  # required.
  def setRequiredNodes(self, graph):

    # Loop over all data nodes.
    for node in graph.nodes(data = False):
      nodeType = self.nodeMethods.getGraphNodeAttribute(graph, node, 'nodeType')
      if nodeType == 'file' or nodeType == 'option':
        for edge in graph.edges(node):
          task           = edge[1]
          associatedTool = self.nodeMethods.getGraphNodeAttribute(graph, task, 'tool')
          toolArgument   = self.edgeMethods.getEdgeAttribute(graph, node, task, 'argument')
          if toolArgument != 'dummy':
            isRequired = self.tools.attributes[associatedTool].arguments[toolArgument].isRequired
            if isRequired: graph.node[node]['attributes'].isRequired = True
            break

  def setTaskNodes(self, graph):

    # Loop over all task nodes.
    for node in graph.nodes(data = False):
      if graph.node[node]['attributes'].nodeType == 'task':
        associatedTool = graph.node[node]['attributes'].tool
        graph.node[node]['attributes'].description = self.tools.attributes[associatedTool].description

  # Check that all of the supplied edges (tool arguments) are present in the graph.
  def checkRequiredTaskConnections(self, graph):
    missingEdges = []

    # Loop over all task nodes and find the required edges.
    for node in graph.nodes(data = False):
      if graph.node[node]['attributes'].nodeType == 'task':
        task           = node
        associatedTool = graph.node[task]['attributes'].tool

        # Loop over all edges for this tool.
        for edge in self.tools.attributes[associatedTool].arguments:

          # Only consider required edges.
          if self.tools.attributes[associatedTool].arguments[edge].isRequired:
            edgeIsDefined = False
  
            # Loop over the input and output nodes of this task and check that an edge corresponding to
            # the required edge exists.  First deal with input nodes.
            predecessorNodes = graph.predecessors(task)
            for predecessorNode in predecessorNodes:
              graphEdge = graph[predecessorNode][task]['attributes'].argument
              if graphEdge == edge:
                edgeIsDefined = True
                break

            # Now loop over the output nodes.
            successorNodes = graph.successors(task)
            for successorNode in successorNodes:
              graphEdge = graph[task][successorNode]['attributes'].argument
              if graphEdge == edge:
                edgeIsDefined = True
                break

            if not edgeIsDefined: missingEdges.append((task, edge))

    return missingEdges

  # Find all of the file nodes without a set value.
  def getUnsetFileNodes(self, graph):
    nodes = []
    for node in graph.nodes(data = False):

      # Check that the node is a file node.
      if self.nodeMethods.getGraphNodeAttribute(graph, node, 'nodeType') == 'file':

        # Check that the node is not a pipeline argument.
        if not self.nodeMethods.getGraphNodeAttribute(graph, node, 'isPipelineArgument'):
          if not self.nodeMethods.getGraphNodeAttribute(graph, node, 'hasValue'): nodes.append(node)

    return nodes

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

  # Construct filename for a node.
  def constructInputFilename(self, graph, node):
    print('\tconstruct filename for', node)
