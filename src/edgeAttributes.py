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
    self.argument  = ''
    self.shortForm = ''
    self.isRequired = False

class edgeClass:
  def __init__(self):
    self.errors = configurationClassErrors()

  # Get an attribute from a graph edge.  Fail with sensible message if the edge or attribute does not exist.
  def getEdgeAttribute(self, graph, sourceNode, targetNode, attribute):
    try:
      value= getattr(graph[sourceNode][targetNode]['attributes'], attribute)
    except:

      # Check if the source node exists.
      if sourceNode not in graph.nodes():
        self.errors.noNodeInGetEdgeAttribute(sourceNode, targetNode, 'source')

      # Check if the target node exists.
      if targetNode not in graph.nodes():
        self.errors.noNodeInGetEdgeAttribute(sourceNode, targetNode, 'target')

      # If there are no attributes associated with the edge.
      if 'attributes' not in graph[sourceNode][targetNode]:
        self.errors.noAttributesForEdge(sourceNode, targetNode)

      # If the requested attribute is not associated with the edge attributes.
      if not hasattr(edgeAttributes(), attribute):
        self.errors.invalidAttributeForEdge(sourceNode, targetNode, attribute)

    return value
