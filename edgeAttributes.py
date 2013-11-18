#!/bin/bash/python

from __future__ import print_function
import networkx as nx
from copy import deepcopy

import configurationClassErrors
from configurationClassErrors import *

import json
import os
import sys

class edgeAttributes:
  def __init__(self):
    self.argument             = ''
    self.commandLineArgument  = ''
    self.includeOnCommandLine = True
    self.isFilenameStub       = False
    self.isGreedy             = False
    self.isInput              = False
    self.isRequired           = False
    self.modifyArgument       = ''
    self.shortForm            = ''

class edgeClass:
  def __init__(self):
    self.errors      = configurationClassErrors()

  # Define all of the edge attributes and add an edge to the graph.
  def addEdge(self, graph, nodeMethods, tools, sourceNodeID, targetNodeID, argument):
 
    # Determine which of the nodeIDs corresponds to a task node and then determine
    # the tool.
    if nodeMethods.getGraphNodeAttribute(graph, sourceNodeID, 'nodeType') == 'task':
      tool = nodeMethods.getGraphNodeAttribute(graph, sourceNodeID, 'tool')
    elif nodeMethods.getGraphNodeAttribute(graph, targetNodeID, 'nodeType') == 'task':
      tool = nodeMethods.getGraphNodeAttribute(graph, targetNodeID, 'tool')

    # If neither of the nodes correspond to a task node, fail.
    else:
      #TODO ERROR
      print('edgeMethods.addEdge')
      self.errors.terminate()

    # Create an attributes class.
    attributes = edgeAttributes()

    # Find the values from the tool configuration file for this argument.
    attributes.argument             = tools.getLongFormArgument(tool, argument)
    attributes.commandLineArgument  = tools.getArgumentData(tool, attributes.argument, 'command line argument')
    attributes.isFilenameStub       = tools.getArgumentData(tool, attributes.argument, 'is filename stub')
    if attributes.isFilenameStub == None: attributes.isFilenameStub = False
    attributes.isInput              = tools.getArgumentData(tool, attributes.argument, 'input')
    attributes.shortForm            = tools.getArgumentData(tool, attributes.argument, 'short form argument')

    # Check if the argument should be written to the comand line or not.
    includeOnCommandLine = tools.getArgumentData(tool, attributes.argument, 'include on command line')
    if includeOnCommandLine != None: attributes.includeOnCommandLine = includeOnCommandLine

    # Check if the argument needs to be modified when written to the command line.
    modifyArgument = tools.getArgumentData(tool, attributes.argument, 'modify argument name on command line')
    if modifyArgument == 'stdout': attributes.modifyArgument = modifyArgument

    # Add the edge to the graph.
    graph.add_edge(sourceNodeID, targetNodeID, attributes = attributes)

  # Get an attribute from a graph edge.  Fail with sensible message if the edge or attribute does not exist.
  def getEdgeAttribute(self, graph, sourceNodeID, targetNodeID, attribute):
    try:
      value = getattr(graph[sourceNodeID][targetNodeID]['attributes'], attribute)
    except:

      # Check if the source node exists.
      if sourceNodeID not in graph:
        self.errors.noNodeInGetEdgeAttribute(sourceNodeID, targetNodeID, 'source')

      # Check if the target node exists.
      if targetNodeID not in graph[sourceNodeID]:
        self.errors.noNodeInGetEdgeAttribute(sourceNodeID, targetNodeID, 'target')

      # If there are no attributes associated with the edge.
      if 'attributes' not in graph[sourceNodeID][targetNodeID]:
        self.errors.noAttributesForEdge(sourceNodeID, targetNodeID)

      # If the requested attribute is not associated with the edge attributes.
      if not hasattr(edgeAttributes(), attribute):
        self.errors.invalidAttributeForEdge(sourceNodeID, targetNodeID, attribute)

    return value

  # Set an edge attribute.
  def setEdgeAttribute(self, graph, sourceNodeID, targetNodeID, attribute, value):
    self.getEdgeAttribute(graph, sourceNodeID, targetNodeID, attribute)
    setattr(graph[sourceNodeID][targetNodeID]['attributes'], attribute, value)

  # Determine if an edge exists between two nodes.
  def checkIfEdgeExists(self, graph, sourceNodeID, targetNodeID):
    try: edge = graph[sourceNodeID][targetNodeID]
    except: return False
    return True
