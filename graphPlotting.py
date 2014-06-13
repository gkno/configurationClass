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

    # Define a list of colours.
    self.colours = [ \
      'darkslategray1', \
      'deepskyblue', \
      'burlywood4', \
      'chocolate', \
      'darkgoldenrod', \
      'darkgoldenrod4', \
      'deepskyblue4', \
      'burlywood', \
      'darkkhaki', \
      'darkolivegreen2', \
      'darkolivegreen4' \
    ]

  # Plot a dot file. Either the option, file or all nodes are plotted (gkno specific
  # nodes are removed).
  def drawDot(self, graph, config, filename):
    graphToDraw = graph.copy()

    # Define a dictionary for containing mapping information for the nodes.  This will
    # map the current node names into new ones.
    mapping = {}

    # Get the required nodes for plotting.
    fileNodeIDs   = config.nodeMethods.getNodes(graphToDraw, 'file')
    optionNodeIDs = config.nodeMethods.getNodes(graphToDraw, 'option')
    gknoNodeIDs   = config.nodeMethods.getNodes(graphToDraw, 'general')
    for nodeID in optionNodeIDs: config.nodeMethods.setGraphNodeAttribute(graphToDraw, nodeID, 'isMarkedForRemoval', True)

    # Find the first file associated with the file node.
    for nodeID in fileNodeIDs:
      try: newNodeID = config.nodeMethods.getGraphNodeAttribute(graph, nodeID, 'values')[1][0]
      except: newNodeID = nodeID

      # Remove the path from the file.
      if '/' in newNodeID: newNodeID = newNodeID.split('/')[-1]

      # Rename the node.
      if nodeID != newNodeID: config.nodeMethods.renameNode(graphToDraw, config.tools, nodeID, newNodeID, allowNullArgument = True)

    # Mark gkno specific nodes for removal.
    for nodeID in gknoNodeIDs: config.nodeMethods.setGraphNodeAttribute(graphToDraw, nodeID, 'isMarkedForRemoval', True)
    graphToDraw.remove_node('gkno')
    config.nodeMethods.purgeNodeMarkedForRemoval(graphToDraw)

    # Loop over all of the task nodes and determine the number times this task is executed. If it is
    # run multiple times, determine if it is first, last or intermediate in a set of tasks running
    # multiple times.
    lastTask        = None
    lastTaskOutputs = 0
    for task in config.pipeline.workflow:
      numberOfInputs  = 0
      numberOfOutputs = 0
      firstTask       = False
      lastTask        = False
      for fileNodeID in config.nodeMethods.getPredecessorFileNodes(graph, task):
        length         = len(config.nodeMethods.getGraphNodeAttribute(graph, fileNodeID, 'values'))
        numberOfInputs = length if length > numberOfInputs else numberOfInputs
      for optionNodeID in config.nodeMethods.getPredecessorOptionNodes(graph, task):
        length         = len(config.nodeMethods.getGraphNodeAttribute(graph, optionNodeID, 'values'))
        numberOfInputs = length if length > numberOfInputs else numberOfInputs
      for fileNodeID in config.nodeMethods.getSuccessorFileNodes(graph, task):
        length         = len(config.nodeMethods.getGraphNodeAttribute(graph, fileNodeID, 'values'))
        numberOfOutputs = length if length > numberOfOutputs else numberOfOutputs

      # If the previous task produced one set of outputs, and this task has multiple inputs, this task is the first
      # (or only) task in a set of linked parallel tasks.
      if lastTaskOutputs == 1 and numberOfInputs > 1:
        for count in range(2, numberOfInputs + 1):
          name = task + ' set ' + str (count)
          graphToDraw.add_node(name, attributes = graph.node[task]['attributes'])
          for fileNodeID in config.nodeMethods.getPredecessorFileNodes(graphToDraw, task):
            fileNodeName = 'set ' + str (count) + fileNodeID
            length       = len(config.nodeMethods.getGraphNodeAttribute(graphToDraw, fileNodeID, 'values'))
            if length == 1: graphToDraw.add_edge(fileNodeID, name, attributes = graphToDraw[fileNodeID][task]['attributes'])
            else:
              graphToDraw.add_node(fileNodeName, attributes = graphToDraw.node[fileNodeID]['attributes'])
              graphToDraw.add_edge(fileNodeName, name, attributes = graphToDraw[fileNodeID][task]['attributes'])

          # Create new successor file nodes.
          for fileNodeID in config.nodeMethods.getSuccessorFileNodes(graphToDraw, task):
            fileNodeName = 'set ' + str(count) + ' ' + fileNodeID
            graphToDraw.add_node(fileNodeName, attributes = graphToDraw.node[fileNodeID]['attributes'])
            graphToDraw.add_edge(name, fileNodeName, attributes = graphToDraw[task][fileNodeID]['attributes'])

      # Tasks in a set of tasks linked together before being rejoined.
      elif numberOfInputs > 1 and numberOfOutputs > 1:
        for count in range(2, numberOfInputs + 1):
          name = task + ' set ' + str (count)
          graphToDraw.add_node(name, attributes = graph.node[task]['attributes'])
          for fileNodeID in config.nodeMethods.getPredecessorFileNodes(graphToDraw, task):
            fileNodeName = 'set ' + str(count) + ' ' + fileNodeID
            length       = len(config.nodeMethods.getGraphNodeAttribute(graphToDraw, fileNodeID, 'values'))

            # If the input file has only one set of data, add an edge to this data.
            if length == 1: graphToDraw.add_edge(fileNodeID, name, attributes = graphToDraw[fileNodeID][task]['attributes'])
            else:
              graphToDraw.add_node(fileNodeName, attributes = graphToDraw.node[fileNodeID]['attributes'])
              graphToDraw.add_edge(fileNodeName, name, attributes = graphToDraw[fileNodeID][task]['attributes'])

          # Create new successor file nodes.
          for fileNodeID in config.nodeMethods.getSuccessorFileNodes(graphToDraw, task):
            fileNodeName = 'set ' + str(count) + ' ' + fileNodeID
            graphToDraw.add_node(fileNodeName, attributes = graphToDraw.node[fileNodeID]['attributes'])
            graphToDraw.add_edge(name, fileNodeName, attributes = graphToDraw[task][fileNodeID]['attributes'])

      # Tasks that pool together multiple inputs into a single output.
      elif numberOfInputs > 1 and numberOfOutputs == 1:
        for fileNodeID in config.nodeMethods.getPredecessorFileNodes(graphToDraw, task):
          length = len(config.nodeMethods.getGraphNodeAttribute(graphToDraw, fileNodeID, 'values'))
          if length != 1:
            for count in range(2, numberOfInputs + 1):
              fileNodeName = 'set ' + str(count) + ' ' + fileNodeID
              graphToDraw.add_edge(fileNodeName, task, attributes = graphToDraw[fileNodeID][task]['attributes'])

      # Update the last task.
      lastTaskOutputs = numberOfOutputs
      lastTask        = task

    # Get all of the categories associated with tools in the pipeline.
    categories = {}
    colourID   = 0
    for nodeID in graphToDraw:

      # Modify task nodes to include colour labels based on the category.
      if config.nodeMethods.getGraphNodeAttribute(graphToDraw, nodeID, 'nodeType') == 'task':
        tool     = config.nodeMethods.getGraphNodeAttribute(graphToDraw, nodeID, 'tool')
        category = config.tools.getGeneralAttribute(tool, 'category')

        # Associate this category with a colour.
        if category not in categories:
          categories[category] = colourID
          colourID += 1

        # Modify the colour of the node.
        del graphToDraw.node[nodeID]['attributes']
        graphToDraw.add_node(nodeID, shape = 'rectangle', style = 'filled', fillcolor = self.colours[categories[category]])

      # Modify option nodes.
      elif config.nodeMethods.getGraphNodeAttribute(graphToDraw, nodeID, 'nodeType') == 'option':
        del graphToDraw.node[nodeID]['attributes']

      # Modify file nodes.
      elif config.nodeMethods.getGraphNodeAttribute(graphToDraw, nodeID, 'nodeType') == 'file':

        # Get the file extension.
        extension = nodeID.split('.')[-1]
        graphToDraw.add_node(nodeID, label = extension)
        del graphToDraw.node[nodeID]['attributes']

    # Modify the edges so that the command line argument is displayed in the graph plot.
    for nodeID in graphToDraw:
      successorNodeIDs = graphToDraw.successors(nodeID)
      for successorNodeID in successorNodeIDs:

        # If the long form argument is 'None', replace the value with 'dependency'. This edge does not represent a
        # typical argument, but the task is dependent on the file. It may be that the file may be evaluated using a
        # defined command or something else, but the edge should be shown to fully represent dependencies.
        argument = config.edgeMethods.getEdgeAttribute(graphToDraw, nodeID, successorNodeID, 'longFormArgument')
        if argument == None: longFormArgument = '"dependency"'
        else: longFormArgument = '"' + argument + '"'
        graphToDraw.remove_edge(nodeID, successorNodeID)
        #graphToDraw.add_edge(nodeID, successorNodeID, label = longFormArgument)
        graphToDraw.add_edge(nodeID, successorNodeID)

    # Add category nodes.
    firstTask          = config.pipeline.workflow[0]
    predecessorNodeIDs = graphToDraw.predecessors(firstTask)
    firstNodeID        = predecessorNodeIDs[0] if predecessorNodeIDs else firstTask
    for category in categories:
      name = 'CAT_' + str(category)
      graphToDraw.add_node(name, shape = 'rectangle', style = 'filled', fillcolor = self.colours[categories[category]], label = category)
      graphToDraw.add_edge(name, firstNodeID, style = 'invis')

    # Map the new node names.
    graphToDraw = nx.relabel_nodes(graphToDraw, {})
    nx.write_dot(graphToDraw, filename)
