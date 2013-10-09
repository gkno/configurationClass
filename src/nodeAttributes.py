#!/bin/bash/python

from __future__ import print_function
import networkx as nx
from copy import deepcopy

import configurationClassErrors
from configurationClassErrors import *

import edgeAttributes
from edgeAttributes import *

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
    self.edgeMethods = edgeClass()
    self.errors      = configurationClassErrors()

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
  def addValuestoGraphNodeAttribute(self, graph, node, values, overwrite):

    # Since values have been added to the node, set the hasValue flag to True.
    self.setGraphNodeAttribute(graph, node, 'hasValue', True)

    # Determine how many sets of values are already included.
    numberOfDataSets = int(self.getGraphNodeAttribute(graph, node, 'numberOfDataSets'))

    # Add the values.  If overwrite is set to True, set the first iteration of values to
    # the given values.  Otherwise, generate a new iteration.
    if not overwrite:
      graph.node[node]['attributes'].values[numberOfDataSets + 1] = values
    else:
      graph.node[node]['attributes'].values[1] = values

    # Set the number of data sets.
    self.setGraphNodeAttribute(graph, node, 'numberOfDataSets', 1)

  # Find all of the task nodes in the graph.
  def getNodes(self, graph, nodeType):
    nodeList = []
    for node in graph.nodes(data = False):
      if self.getGraphNodeAttribute(graph, node, 'nodeType') == nodeType: nodeList.append(node)

    return nodeList

  # Get the node associated with a pipeline argument.
  def getNodeForPipelineArgument(self, graph, argument):
    for node in graph.nodes(data = False):
      if self.getGraphNodeAttribute(graph, node, 'nodeType') != 'task':
        if self.getGraphNodeAttribute(graph, node, 'isPipelineArgument'):
          if self.getGraphNodeAttribute(graph, node, 'argument') == argument: return node

    return None

  # Get the node associated with a tool argument.
  def getNodeForTaskArgument(self, graph, task, argument):
    predecessorEdges = graph.in_edges(task)
    for predecessorEdge in predecessorEdges:
      value = self.edgeMethods.getEdgeAttribute(graph, predecessorEdge[0], predecessorEdge[1], 'argument')
      if value == argument:
        return predecessorEdge[0]

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
