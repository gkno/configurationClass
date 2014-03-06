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
    self.isFilenameStub = False
    self.isGreedy       = False
    self.isInput        = False
    self.isRequired     = False
    self.modifyArgument = None

    # Define the arguments associated with the edge.
    self.longFormArgument  = None
    self.shortFormArgument = None

    # Record how to handle streaming files.
    self.isStreaming      = False
    self.ifInputIsStream  = False
    self.ifOutputIsStream = False
 
    # Record the value to include on the command line (and whether it should be included
    # in the first place).
    self.includeOnCommandLine = True
    self.commandLineArgument  = None

    # It is permissible in some cases to link the output of a tool producing a json file
    # to another task which will read the json at execution time. In this case, there is
    # no argument to set, but the following flag will be set.
    self.readJson = False

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
    attributes.longFormArgument  = tools.getLongFormArgument(tool, argument)
    attributes.shortFormArgument = tools.getArgumentAttribute(tool, attributes.longFormArgument, 'shortFormArgument')

    # Find the command line argument that should be used in the makefile, e.g. that the tool expects.
    # The configuration file may define a different value for consistency across the tools, but the
    # tool itself must be supplied with what it expects.
    if tools.getArgumentAttribute(tool, attributes.longFormArgument, 'commandLineArgument') == None: attributes.commandLineArgument = attributes.longFormArgument
    else: attributes.commandLineArgument = tools.getArgumentAttribute(tool, attributes.longFormArgument, 'commandLineArgument')

    # Identify if the edge represents a filename stub.
    attributes.isFilenameStub = tools.getArgumentAttribute(tool, attributes.longFormArgument, 'isFilenameStub')
    if attributes.isFilenameStub == None: attributes.isFilenameStub = False

    # Determine if the option represents an input file.
    attributes.isInput = tools.getArgumentAttribute(tool, attributes.longFormArgument, 'isInput')

    # Check if the argument should be written to the comand line or not.
    includeOnCommandLine = tools.getArgumentAttribute(tool, attributes.longFormArgument, 'includeOnCommandLine')
    if includeOnCommandLine != None: attributes.includeOnCommandLine = includeOnCommandLine

    # Check if the argument needs to be modified when written to the command line.
    modifyArgument = tools.getArgumentAttribute(tool, attributes.longFormArgument, 'modifyArgument')
    if modifyArgument: attributes.modifyArgument = modifyArgument

    # Define how to handle streaming files.
    attributes.ifOutputIsStream = tools.getArgumentAttribute(tool, attributes.longFormArgument, 'outputStream')
    attributes.ifInputIsStream  = tools.getArgumentAttribute(tool, attributes.longFormArgument, 'inputStream')

    # Add the edge to the graph.
    graph.add_edge(sourceNodeID, targetNodeID, attributes = attributes)

  # If the argument is 'read json file', then this does not refer to an actual tool argument.
  # Instead, the output file associated with this node is in json format and will be read at
  # execution time by this tool to set arguments. If this is the case, just record on this
  # edge the fact that this is a json file.
  def addJsonEdge(self, graph, sourceNodeID, targetNodeID):
    attributes = edgeAttributes()

    # Since this edge does not represent an actual argument, leave the arguments as None. Set the readJson
    # flag to true. Also set the isInput flag as this node must be reading in a json file.
    attributes.readJson          = True
    attributes.isInput           = True
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

  # Determine if an edge exists between two nodes and that the given argument is associated with the edge.
  def checkIfEdgeAssociatedWithArgument(self, graph, sourceNodeID, targetNodeID, argument):
    try: edge = graph[sourceNodeID][targetNodeID]
    except: return False

    # Check that this edge is associated with the given argument.
    edgeArgument = self.getEdgeAttribute(graph, sourceNodeID, targetNodeID, 'longFormArgument')
    if edgeArgument == argument: return True
    else: return False
