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
    self.argument       = ''
    self.isFilenameStub = False
    self.isRequired     = False
    self.shortForm      = ''

class edgeClass:
  def __init__(self):
    self.errors = configurationClassErrors()

  # Get an attribute from a graph edge.  Fail with sensible message if the edge or attribute does not exist.
  def getEdgeAttribute(self, graph, sourceNodeID, targetNodeID, attribute):
    try:
      value = getattr(graph[sourceNodeID][targetNodeID]['attributes'], attribute)
    except:

      # Check if the source node exists.
      if sourceNodeID not in graph.nodes():
        self.errors.noNodeInGetEdgeAttribute(sourceNodeID, targetNodeID, 'source')

      # Check if the target node exists.
      if targetNodeID not in graph.nodes():
        self.errors.noNodeInGetEdgeAttribute(sourceNodeID, targetNodeID, 'target')

      # If there are no attributes associated with the edge.
      if 'attributes' not in graph[sourceNodeID][targetNodeID]:
        self.errors.noAttributesForEdge(sourceNodeID, targetNodeID)

      # If the requested attribute is not associated with the edge attributes.
      if not hasattr(edgeAttributes(), attribute):
        self.errors.invalidAttributeForEdge(sourceNodeID, targetNodeID, attribute)

    return value
