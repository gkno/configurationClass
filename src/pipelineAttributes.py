#!/bin/bash/python

from __future__ import print_function
import networkx as nx
from copy import deepcopy

import configurationClassErrors
from configurationClassErrors import *

import json
import os
import sys

# Define a class for holding attributes for task nodes.
class taskNodeAttributes:
  def __init__(self):
    self.description            = 'No description provided'
    self.isGreedy               = True
    self.nodeType               = 'task'
    self.tool                   = ''

# Define a class for holding attributes for options nodes.  These are nodes that
# hold option data, but are not files.
class optionNodeAttributes:
  def __init__(self):
    self.allowMultipleArguments = False
    self.dataType               = ''
    self.description            = 'No description provided'
    self.hasMultipleDataSets    = False
    self.hasMultipleValues      = False
    self.hasValue               = False
    self.isPipelineArgument     = False
    self.isRequired             = False
    self.nodeType               = 'option'
    self.pipelineArgument       = ''
    self.shortForm              = ''
    self.values                 = {}

# Define a class for holding attributes for file nodes.  These are nodes that
# hold information about files.
class fileNodeAttributes:
  def __init__(self):
    self.allowMultipleArguments = False
    self.description            = 'No description provided'
    self.hasMultipleDataSets    = False
    self.hasMultipleValues      = False
    self.hasValue               = False
    self.isPipelineArgument     = False
    self.isRequired             = False
    self.nodeType               = 'file'
    self.pipelineArgument       = ''
    self.shortForm              = ''
    self.values                 = {}

class edgeAttributes:
  def __init__(self):
    self.argument = ''
    self.isRequired = False

class pipelineConfiguration:
  def __init__(self):
    self.configurationData = {}
    self.description       = 'No description provided'
    self.errors            = configurationClassErrors()
    self.filename          = ''
    self.pipelineName      = ''

  # Open a configuration file and store the contents of the file in the
  # configuration dictionary.
  def readConfigurationFile(self, filename):
    fileExists = False
    jsonError  = True
    errorText  = ''

    try: jsonData = open(filename)
    except: return fileExists, jsonError, errorText
    fileExists    = True
    self.filename = filename

    try: self.configurationData = json.load(jsonData)
    except:
      exc_type, exc_value, exc_traceback = sys.exc_info()
      errorText = exc_value
      return fileExists, jsonError, errorText

    jsonError = False

    return fileExists, jsonError, errorText

  #TODO
  # Check that the pipeline configuration file is valid.  If so, put all the information in
  # the pipeline data structures.
  def validateConfigurationData(self, filename):
    self.description = self.configurationData['description']

    return True

  # Transfer all of the information from the configuration file into data structures.
  def addNodesAndEdges(self, graph, toolData):
    inputNodes  = {}
    outputNodes = {}

    # Set the pipeline arguments.
    for argument in self.configurationData['arguments']:

      # Each new node ID must be unique.  Throw an error if this node ID has been seen before.
      nodeID = self.configurationData['arguments'][argument]['ID']
      if graph.has_node(nodeID):
        print('non-unique argument node: ', nodeID)
        exit(1)

      # Determine if this argument is for a file or an option.  If it is for a file, attach the
      # fileNodeAttributes structure to the node.
      if self.configurationData['arguments'][argument]['file']:
        attributes                    = fileNodeAttributes()
        attributes.argument           = argument
        attributes.description        = self.configurationData['arguments'][argument]['description']
        attributes.isPipelineArgument = True
        if 'required' in self.configurationData['arguments'][argument]: attributes.isRequired = self.configurationData['arguments'][argument]['required'] 
        attributes.shortForm          = self.configurationData['arguments'][argument]['short form']

      # If the argument defines an option, attach the optionNodeAttributes data structure to the
      #node.
      else:
        attributes                    = optionNodeAttributes()
        attributes.argument           = argument
        attributes.description        = self.configurationData['arguments'][argument]['description']
        attributes.isPipelineArgument = True
        if 'required' in self.configurationData['arguments'][argument]: attributes.isRequired = self.configurationData['arguments'][argument]['required'] 
        attributes.shortForm          = self.configurationData['arguments'][argument]['short form']

      # Add the node to the graph.
      graph.add_node(nodeID, attributes = attributes)

    # Loop through all of the tasks and store all the information about the edges.
    for task in self.configurationData['tasks']:

      inputNodes[task]  = []
      outputNodes[task] = []

      # Each new node ID must be unique.  Throw an error if this node ID has been seen before.
      if graph.has_node(task):
        print('non-unique task node: ', task)
        exit(1)

      # Create the new node and attach the relevant information to it.
      attributes      = taskNodeAttributes()
      attributes.tool = self.configurationData['tasks'][task]['tool']
      graph.add_node(task, attributes = attributes)

      # Put all of the input and output nodes into a list, then add all of them to the
      # graph.
      for inputNode in self.configurationData['tasks'][task]['input nodes']: inputNodes[task].append(inputNode)
      for outputNode in self.configurationData['tasks'][task]['output nodes']: outputNodes[task].append(outputNode)

    self.addInputOutputNodes(graph, toolData, inputNodes, 'input nodes')
    self.addInputOutputNodes(graph, toolData, outputNodes, 'output nodes')

  # Add the nodes listed in the 'input nodes' and 'output nodes' section of the pipeline
  # configuration file to the graph.
  def addInputOutputNodes(self, graph, toolData, nodes, nodesListID):
    for task in nodes:
      for node in nodes[task]:

        # Determine the tool that is used to execute this task.  When parsing the input nodes, we
        # will need to identify the data associated with the node (e.g. a file or an option) in
        # order to add a node with the correct attributes data structure.
        associatedTool = self.getNodeAttribute(graph, task, 'tool')

        # If the input node is not already in the graph, add it.
        if not graph.has_node(node):

          # Identify the argument associated with this node.
          if nodesListID == 'input nodes': argument = self.configurationData['tasks'][task]['input nodes'][node]
          else: argument = 'dummy'
          isFile = toolData.isArgumentAFile(associatedTool, argument)
          if isFile:
            nodeAttributes = fileNodeAttributes()
          else:
            nodeAttributes = optionNodeAttributes()
          graph.add_node(node, attributes = nodeAttributes)

        # Add an edge from the input node to the task.
        edge = edgeAttributes()
        if nodesListID == 'input nodes':
          edge.argument = self.configurationData['tasks'][task][nodesListID][node]
          graph.add_edge(node, task, attributes = edge)
        else:
          edge.argument = 'dummy'
          graph.add_edge(task, node, attributes = edge)

  # Erase all of the data contained in the self.configurationData structure.
  def eraseConfigurationData(self):
    self.configurationData = {}

  # Get an attribute from the nodes data structure.  Check to ensure that the requested attribute is
  # available for the type of node.  If not, terminate with an error.
  def getNodeAttribute(self, graph, node, attribute):
    try:
      value = getattr(graph.node[node]['attributes'], attribute)

    # If there is an error, determine if the node exists in the graph.  If the node exists, the problem
    # lies with the attribute.  Determine if the attribute belongs to any of the node data structures,
    # then terminate.
    except:
      if node not in graph.nodes():
        self.errors.missingNodeInAttributeRequest(node)

      # If no attributes have been attached to the node.
      elif 'attributes' not in graph.node[node]:
        self.errors.noAttributesInAttributeRequest(node)

      # If the attribute is not associated with the node.
      else:
        inTaskNode    = False
        inFileNode    = False
        inOptionsNode = False
        if hasattr(taskNodeAttributes(), attribute): inTaskNode      = True
        if hasattr(fileNodeAttributes(), attribute): inFileNode      = True
        if hasattr(optionNodeAttributes(), attribute): inOptionsNode = True
        if graph.node[node]['attributes'].nodeType == 'task': nodeType = 'task node'
        if graph.node[node]['attributes'].nodeType == 'file': nodeType = 'file node'
        if graph.node[node]['attributes'].nodeType == 'option': nodeType = 'options node'
        self.errors.attributeNotAssociatedWithNode(node, attribute, nodeType, inTaskNode, inFileNode, inOptionsNode)

    return value

  # Set an attribute from the nodes data structure.  Check to ensure that the requested attribute is
  # available for the type of node.  If not, terminate with an error.
  def setNodeAttribute(self, graph, node, attribute, value):
    try:
      test = getattr(graph.node[node]['attributes'], attribute)

    # If there is an error, determine if the node exists in the graph.  If the node exists, the problem
    # lies with the attribute.  Determine if the attribute belongs to any of the node data structures,
    # then terminate.
    except:
      if node not in graph.nodes():
        self.errors.missingNodeInAttributeSet(node)

      # If no attributes have been attached to the node.
      elif 'attributes' not in graph.node[node]:
        self.errors.noAttributesInAttributeSet(node)

      # If the attribute is not associated with the node.
      else:
        inTaskNode    = False
        inFileNode    = False
        inOptionsNode = False
        if hasattr(taskNodeAttributes(), attribute): inTaskNode      = True
        if hasattr(fileNodeAttributes(), attribute): inFileNode      = True
        if hasattr(optionNodeAttributes(), attribute): inOptionsNode = True
        if graph.node[node]['attributes'].nodeType == 'task': nodeType = 'task node'
        if graph.node[node]['attributes'].nodeType == 'file': nodeType = 'file node'
        if graph.node[node]['attributes'].nodeType == 'option': nodeType = 'options node'
        self.errors.attributeNotAssociatedWithNodeInSet(node, attribute, nodeType, inTaskNode, inFileNode, inOptionsNode)

    setattr(graph.node[node]['attributes'], attribute, value)

  # Generate the task workflow from the topologically sorted pipeline graph.
  def generateWorkflow(self, graph):
    workflow  = []
    topolSort = nx.topological_sort(graph)
    for node in topolSort:
      nodeType = self.getNodeAttribute(graph, node, 'nodeType')
      if nodeType == 'task': workflow.append(node)

    return workflow

  # Set all task node attributes.
  def getRequiredTools(self, graph):
    tools = []
    for node in graph.nodes(data = False):

      # Find the tool used by this task.
      nodeType = self.getNodeAttribute(graph, node, 'nodeType')
      if nodeType == 'task':
        tool = self.getNodeAttribute(graph, node, 'tool')
        tools.append(tool)

    return tools

  # Check each data node and determine if a value is required.  This can be determined in one of two
  # ways.  If any of the edges beginning at the data node correspond to a tool argument that is
  # listed as required by the tool, or if the node corresponds to a command line argument that is
  # listed as required.  If the node is a required pipeline argument, it has already been tagged as
  # required.
  def setRequiredNodes(self, graph, toolData):

    # Loop over all data nodes.
    for node in graph.nodes(data = False):
      nodeType = self.getNodeAttribute(graph, node, 'nodeType')
      if nodeType == 'file' or nodeType == 'option':
        for edge in graph.edges(node):
          task           = edge[1]
          associatedTool = self.getNodeAttribute(graph, task, 'tool')
          toolArgument   = graph[node][task]['attributes'].argument
          if toolArgument != 'dummy':
            isRequired = toolData.attributes[associatedTool].arguments[toolArgument].isRequired
            if isRequired: graph.node[node]['attributes'].isRequired = True
            break

  def setTaskNodes(self, graph, toolData):

    # Loop over all task nodes.
    for node in graph.nodes(data = False):
      if graph.node[node]['attributes'].nodeType == 'task':
        associatedTool = graph.node[node]['attributes'].tool
        graph.node[node]['attributes'].description = toolData.attributes[associatedTool].description

  # Check that all of the supplied edges (tool arguments) are present in the graph.
  def checkRequiredTaskConnections(self, graph, toolData):
    missingEdges = []

    # Loop over all task nodes and find the required edges.
    for node in graph.nodes(data = False):
      if graph.node[node]['attributes'].nodeType == 'task':
        task           = node
        associatedTool = graph.node[task]['attributes'].tool

        # Loop over all edges for this tool.
        for edge in toolData.attributes[associatedTool].arguments:

          # Only consider required edges.
          if toolData.attributes[associatedTool].arguments[edge].isRequired:
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

  # Find all of the task nodes in the graph.
  def getNodes(self, graph, nodeType):
    nodeList = []
    for node in graph.nodes(data = False):
      if self.getNodeAttribute(graph, node, 'nodeType') == nodeType: nodeList.append(node)

    return nodeList

  # Get the node associated with a pipeline argument.
  def getNodeForPipelineArgument(self, graph, argument):
    for node in graph.nodes(data = False):
      if self.getNodeAttribute(graph, node, 'nodeType') != 'task':
        if self.getNodeAttribute(graph, node, 'isPipelineArgument'):
          if self.getNodeAttribute(graph, node, 'argument') == argument: return node

    return None
