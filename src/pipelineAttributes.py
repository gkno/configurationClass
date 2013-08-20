#!/bin/bash/python

from __future__ import print_function
import networkx as nx
from copy import deepcopy

import json
import os
import sys

class nodeAttributes:
  def __init__(self):
    self.allowMultipleArguments = False
    self.argument               = ''
    self.dataType               = ''
    self.description            = 'No description provided'
    self.hasValue               = False
    self.hasMultipleValues      = False
    self.isGreedy               = True
    self.isPipelineArgument     = False
    self.isRequired             = False
    self.nodeType               = ''
    self.shortForm              = ''
    self.tool                   = ''

class edgeAttributes:
  def __init__(self):
    self.argument = ''
    self.isRequired = False

class pipelineConfiguration:
  def __init__(self):
    self.configurationData = {}
    self.description       = 'No description provided'
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
  def addNodesAndEdges(self, graph):

    # Set the pipeline arguments.
    for argument in self.configurationData['arguments']:

      # Each new node ID must be unique.  Throw an error if this node ID has been seen before.
      nodeID = self.configurationData['arguments'][argument]['ID']
      if graph.has_node(nodeID):
        print('non-unique argument node: ', nodeID)
        exit(1)

      attributes                    = nodeAttributes()
      attributes.argument           = argument
      attributes.description        = self.configurationData['arguments'][argument]['description']
      attributes.isPipelineArgument = True
      attributes.nodeType           = 'data'
      attributes.required           = False
      if 'required' in self.configurationData['arguments'][argument]: attributes.isRequired = self.configurationData['arguments'][argument]['required'] 
      attributes.shortForm   = self.configurationData['arguments'][argument]['short form']
      graph.add_node(nodeID, attributes = attributes)

    # Loop through all of the tasks and store all the information about the edges.
    for task in self.configurationData['tasks']:

      # Each new node ID must be unique.  Throw an error if this node ID has been seen before.
      if graph.has_node(task):
        print('non-unique task node: ', task)
        exit(1)

      # Create the new node and attach the relevant information to it.
      attributes          = nodeAttributes()
      attributes.nodeType = 'task'
      attributes.tool     = self.configurationData['tasks'][task]['tool']
      for inputNode in self.configurationData['tasks'][task]['input nodes']: 

        # If the input node is not already in the graph, add it.
        if not graph.has_node(inputNode):
          dataNodeAttributes                   = nodeAttributes()
          dataNodeAttributes.nodeType          = 'data'
          graph.add_node(inputNode, attributes = dataNodeAttributes)

        # Add an edge from the input node to the task.
        edge = edgeAttributes()
        edge.argument = self.configurationData['tasks'][task]['input nodes'][inputNode]
        graph.add_edge(inputNode, task, attributes = edge)

      # Now add output nodes and draw connections.
      for outputNode in self.configurationData['tasks'][task]['output nodes']:

        # If the input node is not already in the graph, add it.
        if not graph.has_node(outputNode):
          dataNodeAttibutes                     = nodeAttributes()
          dataNodeAttibutes.nodeType            = 'data'
          graph.add_node(outputNode, attributes = dataNodeAttibutes)

        # Add an edge from the input node to the task.
        edge = edgeAttributes()
        edge.argument = 'dummy'
        graph.add_edge(task, outputNode, attributes = edge)

      graph.add_node(task, attributes = attributes)

    self.configurationData = {}

  # Generate the task workflow from the topologically sorted pipeline graph.
  def generateWorkflow(self, graph):
    workflow  = []
    topolSort = nx.topological_sort(graph)
    for node in topolSort:
      if graph.node[node]['attributes'].nodeType == 'task': workflow.append(node)

    return workflow

  # Set all task node attributes.
  def getRequiredTools(self, graph):
    tools = []
    for node in graph.nodes(data = False):

      # Find the tool used by this task.
      if graph.node[node]['attributes'].nodeType == 'task': tools.append(graph.node[node]['attributes'].tool)

    return tools

  # Check each data node and detemine if a value is required.  This can be determined in one of two
  # ways.  If any of the edges beginning at the data node correspond to a tool argument that is
  # listed as required by the tool, or if the node corresponds to a command line argument that is
  # listed as required.  If the node is a required pipeline argument, it has already been tagged as
  # required.
  def setRequiredNodes(self, graph, toolData):

    # Loop over all data nodes.
    for node in graph.nodes(data = False):
      if graph.node[node]['attributes'].nodeType == 'data':
        for edge in graph.edges(node):
          task           = edge[1]
          associatedTool = graph.node[task]['attributes'].tool
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

