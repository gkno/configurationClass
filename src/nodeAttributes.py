#!/bin/bash/python

from __future__ import print_function
import networkx as nx
from copy import deepcopy

import configurationClassErrors
from configurationClassErrors import *

import edgeAttributes
from edgeAttributes import *

import json
import operator
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
    self.allowedExtensions   = []
    self.allowMultipleValues = False
    self.associatedFileNodes = []
    self.dataType            = ''
    self.description         = 'No description provided'
    self.hasMultipleDataSets = False
    self.hasMultipleValues   = False
    self.hasValue            = False
    self.isFile              = False
    self.isInput             = False
    self.isOutput            = False
    self.isPipelineArgument  = False
    self.isRequired          = False
    self.nodeType            = 'option'
    self.numberOfDataSets    = 0
    self.values              = {}

# Define a class for holding attributes for file nodes.  These are nodes that
# hold information about files.
class fileNodeAttributes:
  def __init__(self):
    self.allowMultipleValues = False
    self.allowedExtensions   = []
    self.description         = 'No description provided'
    self.hasMultipleDataSets = False
    self.hasMultipleValues   = False
    self.hasValue            = False
    self.nodeType            = 'file'
    self.numberOfDataSets    = 0
    self.values              = {}
 
class nodeClass:
  def __init__(self):
    self.edgeMethods  = edgeClass()
    self.errors       = configurationClassErrors()
    self.optionNodeID = 1

  # Build an option node.
  def buildOptionNode(self, graph, tools, task, tool, argument, attributes):
    nodeID = str('OPTION_') + str(self.optionNodeID)
    self.optionNodeID += 1
    graph.add_node(nodeID, attributes = attributes)

    # Add an edge to the task node.
    edge          = edgeAttributes()
    edge.argument = argument
    graph.add_edge(nodeID, task, attributes = edge)

    # If the node represents an option for defining an input or output file, create
    # a file node.
    shortForm = tools.getArgumentData(tool, argument, 'short form argument')
    if self.getGraphNodeAttribute(graph, nodeID, 'isInput'): self.buildTaskFileNodes(graph, nodeID, task, argument, shortForm, 'input')
    elif self.getGraphNodeAttribute(graph, nodeID, 'isOutput'): self.buildTaskFileNodes(graph, nodeID, task, argument, shortForm, 'output')

    return nodeID

  # Build a node using information from the tool configuration file.
  def buildNodeFromToolConfiguration(self, tools, tool, argument):

    # Set the tool argument information.
    contents   = tools.configurationData[tool]['arguments'][argument]
    attributes = optionNodeAttributes()
    self.setNodeAttribute(attributes, 'dataType', contents['type'])
    self.setNodeAttribute(attributes, 'description', contents['description'])
    self.setNodeAttribute(attributes, 'isInput', contents['input'])
    self.setNodeAttribute(attributes, 'isOutput', contents['output'])
    if contents['input'] or contents['output']: self.setNodeAttribute(attributes, 'isFile', True)
    self.setNodeAttribute(attributes, 'isRequired', contents['required'])
    if 'allow multiple values' in contents: self.setNodeAttribute(attributes, 'allowMultipleValues', contents['allow multiple values'])

    # If multiple extensions are allowed, they will be separated by pipes in the configuration
    # file.  Add all allowed extensions to the list.
    extension = contents['extension']
    if '|' in extension:
      extensions = extension.split('|')
      self.setNodeAttribute(attributes, 'allowedExtensions', extensions)

    #else: attributes.allowedExtensions.append(extension)
    else:
      extensions = []
      extensions.append(extension)
      self.setNodeAttribute(attributes, 'allowedExtensions', extensions)

    return attributes

  # Add input file nodes.
  def buildTaskFileNodes(self, graph, nodeID, task, argument, shortForm, fileType):

    # TODO DEAL WITH MULTIPLE FILE NODES.
    attributes                     = fileNodeAttributes()
    attributes.description         = self.getGraphNodeAttribute(graph, nodeID, 'description')
    attributes.allowMultipleValues = self.getGraphNodeAttribute(graph, nodeID, 'allowMultipleValues')
    attributes.allowedExtensions   = self.getGraphNodeAttribute(graph, nodeID, 'allowedExtensions')
    fileNodeID                     = nodeID + '_FILE'
    graph.add_node(fileNodeID, attributes = attributes)

    # Add the edge.
    edge           = edgeAttributes()
    edge.argument  = argument
    edge.shortForm = shortForm
    if fileType == 'input': graph.add_edge(fileNodeID, task, attributes = edge)
    elif fileType == 'output': graph.add_edge(task, fileNodeID, attributes = edge)

    # Add the file node to the list of file nodes associated with the option node.
    self.setGraphNodeAttribute(graph, nodeID, 'associatedFileNodes', fileNodeID)

  # TODO FINISH THIS
  # Build a task node.
  def buildTaskNode(self, graph, tools, task, tool):
    attributes             = taskNodeAttributes()
    attributes.tool        = tool
    attributes.description = tools.getConfigurationData(attributes.tool, 'description')
    graph.add_node(task, attributes = attributes)

  # Build all of the predecessor nodes for the task and attach them to the task node.
  def buildRequiredPredecessorNodes(self, graph, tools, task, tool):
    for argument in tools.configurationData[tool]['arguments']:
      attributes = self.buildNodeFromToolConfiguration(tools, tool, argument)
      isRequired = self.getNodeAttribute(attributes, 'isRequired')
      if isRequired: self.buildOptionNode(graph, tools, task, tool, argument, attributes)

  # Check each option node and determine if a value is required.  This can be determined in one of two 
  # ways.  If any of the edges beginning at the option node correspond to a tool argument that is 
  # listed as required by the tool, or if the node corresponds to a command line argument that is 
  # listed as required.  If the node is a required pipeline argument, it has already been tagged as 
  # required. 
  def setRequiredNodes(self, graph, tools): 
 
    # Loop over all data nodes. 
    for nodeID in graph.nodes(data = False): 
      nodeType = self.getGraphNodeAttribute(graph, nodeID, 'nodeType') 
      if nodeType == 'option': 
        for edge in graph.edges(nodeID): 
          task           = edge[1] 
          associatedTool = self.getGraphNodeAttribute(graph, task, 'tool') 
          toolArgument   = self.edgeMethods.getEdgeAttribute(graph, nodeID, task, 'argument') 
          isRequired     = tools.getArgumentData(associatedTool, toolArgument, 'required') 
          if isRequired: self.setGraphNodeAttribute(graph, nodeID, 'isRequired', True) 
          break

  # Check if a node exists based on a task and an argument.
  def doesNodeExist(self, graph, task, argument):
    exists = False
    for sourceNode, targetNode in graph.in_edges(task):
      edgeArgument = self.edgeMethods.getEdgeAttribute(graph, sourceNode, targetNode, 'argument')
      nodeType     = self.getGraphNodeAttribute(graph, sourceNode, 'nodeType')
      if nodeType == 'option' and edgeArgument == argument:
        exists = True
        return sourceNode

    return None

  # Determine which nodes each pipeline argument points to.
  def getPipelineArgumentNodes(self, graph, pipeline, tools):
    for argument in pipeline.argumentData:
      nodeID           = pipeline.getArgumentData(argument, 'nodeID')
      task             = pipeline.nodeTaskInformation[nodeID][0][0]
      taskArgument     = pipeline.nodeTaskInformation[nodeID][0][1]
      tool             = pipeline.tasks[task]
      foundArgument    = False
      predecessorNodes = self.getPredecessorOptionNodes(graph, task)
      for predecessorNodeID in predecessorNodes:
        testArgument = self.edgeMethods.getEdgeAttribute(graph, predecessorNodeID, task, 'argument')
        if taskArgument == testArgument:
          foundArgument = True
          break

      # If the node to which this argument points has not been defined, no edge linking the node
      # to the task will be found.  Thus, the node and edge need to be created.
      if not foundArgument:
        attributes = self.buildNodeFromToolConfiguration(tools, tool, taskArgument)
        predecessorNodeID = self.buildOptionNode(graph, tools, task, tool, taskArgument, attributes)

      # Set the nodeID to a graph nodeID.
      pipeline.argumentData[argument].nodeID = predecessorNodeID

  # Get an attribute from the nodes data structure.  Check to ensure that the requested attribute is
  # available for the type of node.  If not, terminate with an error.
  def getGraphNodeAttribute(self, graph, node, attribute):
    try: value = getattr(graph.node[node]['attributes'], attribute)

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
  def setGraphNodeAttribute(self, graph, nodeID, attribute, value):
    try:
      test = getattr(graph.node[nodeID]['attributes'], attribute)

    # If there is an error, determine if the node exists in the graph.  If the node exists, the problem
    # lies with the attribute.  Determine if the attribute belongs to any of the node data structures,
    # then terminate.
    except:
      if nodeID not in graph.nodes():
        self.errors.missingNodeInAttributeSet(nodeID)

      # If no attributes have been attached to the node.
      elif 'attributes' not in graph.node[nodeID]:
        self.errors.noAttributesInAttributeSet(nodeID)

      # If the attribute is not associated with the node.
      else:
        inTaskNode    = False
        inFileNode    = False
        inOptionsNode = False
        if hasattr(taskNodeAttributes(), attribute): inTaskNode      = True
        if hasattr(fileNodeAttributes(), attribute): inFileNode      = True
        if hasattr(optionNodeAttributes(), attribute): inOptionsNode = True
        if graph.node[nodeID]['attributes'].nodeType == 'task': nodeType = 'task node'
        if graph.node[nodeID]['attributes'].nodeType == 'file': nodeType = 'file node'
        if graph.node[nodeID]['attributes'].nodeType == 'option': nodeType = 'options node'
        self.errors.attributeNotAssociatedWithNodeInSet(nodeID, attribute, nodeType, inTaskNode, inFileNode, inOptionsNode)

    # Set the attribute.  If the attribute points to a list, append the value.
    if type(getattr(graph.node[nodeID]['attributes'], attribute)) == list:
      valueList = getattr(graph.node[nodeID]['attributes'], attribute)
      valueList.append(value)
      setattr(graph.node[nodeID]['attributes'], attribute, valueList)
    else:
      setattr(graph.node[nodeID]['attributes'], attribute, value)

  # Get an attribute from the nodes data structure.  Check to ensure that the requested attribute is
  # available for the type of node.  If not, terminate with an error.  This method is for a node not
  # contained in the graph.
  def getNodeAttribute(self, nodeAttributes, attribute):
    try: value = getattr(nodeAttributes, attribute)

    # If there is an error, determine if the node exists in the graph.  If the node exists, the problem
    # lies with the attribute.  Determine if the attribute belongs to any of the node data structures,
    # then terminate.
    except:

      # If the attribute is not associated with the node.
      inTaskNode    = False
      inFileNode    = False
      inOptionsNode = False
      if hasattr(taskNodeAttributes(), attribute): inTaskNode      = True
      if hasattr(fileNodeAttributes(), attribute): inFileNode      = True
      if hasattr(optionNodeAttributes(), attribute): inOptionsNode = True
      if nodeAttributes.nodeType == 'task': nodeType = 'task node'
      if nodeAttributes.nodeType == 'file': nodeType = 'file node'
      if nodeAttributes.nodeType == 'option': nodeType = 'options node'
      self.errors.attributeNotAssociatedWithNodeNoGraph(attribute, nodeType, inTaskNode, inFileNode, inOptionsNode)

    return value

  # Set an attribute from the nodes data structure.  In this method, the node is not a part of the graph and
  # so the node itself is given to the method.
  def setNodeAttribute(self, nodeAttributes, attribute, value):
    try: test = getattr(nodeAttributes, attribute)

    # If there is an error, determine if the attribute belongs to any of the node data structures,
    # then terminate.
    except:

      # If the attribute is not associated with the node.
      inTaskNode    = False
      inFileNode    = False
      inOptionsNode = False
      if hasattr(taskNodeAttributes(), attribute): inTaskNode      = True
      if hasattr(fileNodeAttributes(), attribute): inFileNode      = True
      if hasattr(optionNodeAttributes(), attribute): inOptionsNode = True
      if nodeAttributes.nodeType == 'task': nodeType = 'task node'
      if nodeAttributes.nodeType == 'file': nodeType = 'file node'
      if nodeAttributes.nodeType == 'option': nodeType = 'options node'
      self.errors.attributeNotAssociatedWithNodeInSetNoGraph(attribute, nodeType, inTaskNode, inFileNode, inOptionsNode)

    setattr(nodeAttributes, attribute, value)

  # Add values to a node.
  def addValuestoGraphNodeAttribute(self, graph, nodeID, values, overwrite):

    # Check if the node value has already been set.  If so, and overwrite is set to False,
    # do not proceed with adding the values.
    hasValue = self.getGraphNodeAttribute(graph, nodeID, 'hasValue')
    if not hasValue or overwrite:

      # Since values have been added to the node, set the hasValue flag to True.
      self.setGraphNodeAttribute(graph, nodeID, 'hasValue', True)
  
      # Determine how many sets of values are already included.
      numberOfDataSets = int(self.getGraphNodeAttribute(graph, nodeID, 'numberOfDataSets'))
  
      # Add the values.  If overwrite is set to True, set the first iteration of values to
      # the given values.  Otherwise, generate a new iteration.
      if not overwrite:
        graph.node[nodeID]['attributes'].values[numberOfDataSets + 1] = values
      else:
        graph.node[nodeID]['attributes'].values[1] = values
  
      # Set the number of data sets.
      self.setGraphNodeAttribute(graph, nodeID, 'numberOfDataSets', 1)

  # Find all of the task nodes in the graph.
  def getNodes(self, graph, nodeType):
    nodeList = []
    for node in graph.nodes(data = False):
      if self.getGraphNodeAttribute(graph, node, 'nodeType') == nodeType: nodeList.append(node)

    return nodeList

  # Get the node associated with a tool argument.
  def getNodeForTaskArgument(self, graph, task, argument):
    predecessorEdges = graph.in_edges(task)
    for predecessorEdge in predecessorEdges:
      value = self.edgeMethods.getEdgeAttribute(graph, predecessorEdge[0], predecessorEdge[1], 'argument')
      if value == argument:

        # Only return a value if this is an option node.
        if self.getGraphNodeAttribute(graph, predecessorEdge[0], 'nodeType') == 'option': return predecessorEdge[0]

    return None

  # Get all predecessor file nodes for a task.
  def getPredecessorOptionNodes(self, graph, task):
    optionNodes = []

    try: predecessors = graph.predecessors(task)
    except:

      #TODO SORT OUT ERROR MESSAGE.
      print('failed')

    for predecessor in predecessors:
      if self.getGraphNodeAttribute(graph, predecessor, 'nodeType') == 'option': optionNodes.append(predecessor)

    return optionNodes

  # Get all predecessor file nodes for a task.
  def getPredecessorFileNodes(self, graph, task):
    fileNodes = []

    try: predecessors = graph.predecessors(task)
    except:

      #TODO SORT OUT ERROR MESSAGE.
      print('failed')

    for predecessor in predecessors:
      if self.getGraphNodeAttribute(graph, predecessor, 'nodeType') == 'file': fileNodes.append(predecessor)

    return fileNodes

  # Get all successor file nodes for a task.
  def getSuccessorFileNodes(self, graph, task):
    fileNodes = []

    try: successors = graph.successors(task)
    except:

      #TODO SORT OUT ERROR MESSAGE.
      print('failed')

    for successor in successors:
      if self.getGraphNodeAttribute(graph, successor, 'nodeType') == 'file': fileNodes.append(successor)

    return fileNodes

  # Given a file node ID, return the corresponding option node ID.
  def getOptionNodeIDFromFileNodeID(self, nodeID):
    optionNodeID = nodeID.replace('_FILE', '')
    return optionNodeID
