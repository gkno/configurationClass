#!/bin/bash/python

from __future__ import print_function
import networkx as nx

import configurationClassErrors
from configurationClassErrors import *

import os
import sys

# Define the plotting class.
class drawGraph():
  def __init__(self):
    self.errors = configurationClassErrors()

  # Plot a dot file. Either the option, file or all nodes are plotted (gkno specific
  # nodes are removed).
  def drawDot(self, graph, nodeMethods, edgeMethods, tools, filename, nodes = 'all'):
    graphToDraw = graph.copy()

    # Define a dictionary for containing mapping information for the nodes.  This will
    # map the current node names into new ones.
    mapping = {}

    # Get the required nodes for plotting.
    fileNodeIDs   = nodeMethods.getNodes(graphToDraw, 'file')
    optionNodeIDs = nodeMethods.getNodes(graphToDraw, 'option')
    gknoNodeIDs   = nodeMethods.getNodes(graphToDraw, 'general')
    if nodes == 'file':
      for nodeID in optionNodeIDs: nodeMethods.setGraphNodeAttribute(graphToDraw, nodeID, 'isMarkedForRemoval', True)

      # Find the first file associated with the file node.
      for nodeID in fileNodeIDs:
        try: newNodeID = nodeMethods.getGraphNodeAttribute(graph, nodeID, 'values')[1][0]
        except: newNodeID = nodeID

        # Rename the node.
        if nodeID != newNodeID: nodeMethods.renameNode(graphToDraw, tools, nodeID, newNodeID, allowNullArgument = True)

    elif nodes == 'option':
      for nodeID in fileNodeIDs: nodeMethods.setGraphNodeAttribute(graphToDraw, nodeID, 'isMarkedForRemoval', True)
    elif nodes != 'all':
      print('Do not know which nodes to plot - graphPlot')
      self.errors.terminate()

    # Mark gkno specific nodes for removal.
    for nodeID in gknoNodeIDs: nodeMethods.setGraphNodeAttribute(graphToDraw, nodeID, 'isMarkedForRemoval', True)
    nodeMethods.purgeNodeMarkedForRemoval(graphToDraw)

    for nodeID in graphToDraw.nodes(data = False):
      if nodeMethods.getGraphNodeAttribute(graphToDraw, nodeID, 'nodeType') == 'option':
        task            = graphToDraw.successors(nodeID)[0]
        newNodeID       = nodeID + str(edgeMethods.getEdgeAttribute(graphToDraw, nodeID, task, 'argument'))
        mapping[nodeID] = newNodeID

    # Modify the edges so that the command line argument is displayed in the graph plot.
    for nodeID in graphToDraw:
      successorNodeIDs = graphToDraw.successors(nodeID)
      for successorNodeID in successorNodeIDs:

        # If the long form argument is 'None', replace the value with 'dependency'. This edge does not represent a
        # typical argument, but the task is dependent on the file. It may be that the file may be evaluated using a
        # defined command or something else, but the edge should be shown to fully represent dependencies.
        argument = edgeMethods.getEdgeAttribute(graphToDraw, nodeID, successorNodeID, 'longFormArgument')
        if argument == None: longFormArgument = '"dependency"'
        else: longFormArgument = '"' + argument + '"'
        graphToDraw.remove_edge(nodeID, successorNodeID)
        graphToDraw.add_edge(nodeID, successorNodeID, label = longFormArgument)

    # Map the new node names.
    graphToDraw = nx.relabel_nodes(graphToDraw, mapping)
    nx.write_dot(graphToDraw, filename)
