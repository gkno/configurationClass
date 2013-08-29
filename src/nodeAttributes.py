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
    self.allowMultipleValues = False
    self.argument            = ''
    self.dataType            = ''
    self.description         = 'No description provided'
    self.hasMultipleDataSets = False
    self.hasMultipleValues   = False
    self.hasValue            = False
    self.isPipelineArgument  = False
    self.isRequired          = False
    self.nodeType            = 'option'
    self.numberOfDataSets    = 0
    self.shortForm           = ''
    self.values              = {}

# Define a class for holding attributes for file nodes.  These are nodes that
# hold information about files.
class fileNodeAttributes:
  def __init__(self):
    self.allowMultipleValues = False
    self.allowedExtensions   = []
    self.argument            = ''
    self.description         = 'No description provided'
    self.hasMultipleDataSets = False
    self.hasMultipleValues   = False
    self.hasValue            = False
    self.isInput             = False
    self.isOutput            = False
    self.isPipelineArgument  = False
    self.isRequired          = False
    self.nodeType            = 'file'
    self.numberOfDataSets    = 0
    self.shortForm           = ''
    self.values              = {}

class nodeClass:
  def __init__(self):
    self.edgeMethods = edgeClass()
    self.errors      = configurationClassErrors()

  # Get an attribute from the nodes data structure.  Check to ensure that the requested attribute is
  # available for the type of node.  If not, terminate with an error.
  def getGraphNodeAttribute(self, graph, node, attribute):
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
  def setGraphNodeAttribute(self, graph, node, attribute, value):
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

  # Set an attribute from the nodes data structure.  In this method, the node is not a part of the graph and
  # so the node itself is given to the method.
  def setNodeAttribute(self, nodeAttributes, attribute, value):
    try:
      test = getattr(nodeAttributes, attribute)

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
