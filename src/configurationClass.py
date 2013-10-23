#!/bin/bash/python

from __future__ import print_function
import networkx as nx
from copy import deepcopy

import configurationClassErrors
from configurationClassErrors import *

import edgeAttributes
from edgeAttributes import *

import fileOperations
from fileOperations import *

import nodeAttributes
from nodeAttributes import *

import pipelineAttributes
from pipelineAttributes import *

import toolAttributes
from toolAttributes import *

import json
import os
import sys

class configurationClass:
  def __init__(self):
    self.edgeMethods       = edgeClass()
    self.errors            = configurationClassErrors()
    self.nodeIDs           = {}
    self.nodeMethods       = nodeClass()
    self.fileOperations    = fileOperations()
    self.pipeline          = pipelineConfiguration()
    self.tools             = toolConfiguration()

  # Build a graph for an individual task.  The pipeline is built by merging nodes between
  # different tasks.  This step is performed later.
  def buildTaskGraph(self, graph):

    for task in self.pipeline.tasks:
      tool = self.pipeline.tasks[task]

      # Generate the task node.
      self.nodeMethods.buildTaskNode(graph, self.tools, task, tool)

      # Find all required arguments for this task and build nodes for them all.  Link these nodes
      # to the task node.
      self.nodeMethods.buildRequiredPredecessorNodes(graph, self.tools, task, tool)

      # TODO ENSURE THAT ADDITIONAL FILES, E.G. STUBS AND INDEXES ARE INCLUDED.

  # Merge shared nodes between tasks using information from the pipeline configuration file.  For
  # example, task A outputs a file fileA and taskB uses this as input.  Having built an individual
  # graph for each task, there exists an output file node for taskA and an input file node for
  # taskB (and also option nodes defining the names), but these are the same file and so these
  # nodes can be merged into a single node.
  def mergeNodes(self, graph):

    # The first step involves parsing through the 'nodes' section of the pipeline configuration file and
    # determining which option nodes will be merged.  For each set of option nodes to be merged, one is
    # picked to be kept and the others are marked for deletion.
    edgesToCreate = self.identifyNodesToRemove(graph)

#    for nodeID in graph.nodes(data = False):
#      if self.nodeMethods.getGraphNodeAttribute(graph, nodeID, 'nodeType') == 'option':
#        if self.nodeMethods.getGraphNodeAttribute(graph, nodeID, 'isMarkedForRemoval'):
#          successorNodeID = graph.successors(nodeID)[0]
#          edge            = self.edgeMethods.getEdgeAttribute(graph, nodeID, successorNodeID, 'argument')
#          print("TEST", nodeID, 'is marked for removal', successorNodeID, edge)
#
#    print('edges')
#    for edge in edgesToCreate: print(edge, edgesToCreate[edge])
#    exit(0)

    # Having completed the merging process, purge the nodes that are no longer required.
    self.nodeMethods.purgeNodeMarkedForRemoval(graph)
    exit(0)


    for nodeName in self.pipeline.nodeTaskInformation:

      # The configuration file lists all the tasks (and arguments) that use the node.  The nodes
      # themselves may have already been placed in the graph, or may not be present or are present
      # for some of the tasks but not others (for example, it may be that only nodes listed as
      # required appear in the graph).  For each task/argument pair, determine if the node exists.
      existingNodes = {}
      forSorting    = {}
      for task, argument in self.pipeline.nodeTaskInformation[nodeName]:
        existingNodes[str(task) + str(argument)] = (task, argument, self.nodeMethods.doesNodeExist(graph, task, argument))
        forSorting[str(task) + str(argument)]    = self.nodeMethods.doesNodeExist(graph, task, argument)

      # Sort the nodes by the nodeID. All values that are None will appear at the beginning.
      #sortedExistingNodes = sorted(existingNodes.iteritems(), key=operator.itemgetter(1))
      sortedNodes = sorted(forSorting.iteritems(), key=operator.itemgetter(1))
      sortedExistingNodes = []
      for ID in sortedNodes: sortedExistingNodes.append(existingNodes[ID[0]])

      # The last item is the node to be kept.  If its value is None, none of the nodes exist and
      # a node with all connections needs to be created.
      lastNode = sortedExistingNodes.pop(len(sortedExistingNodes) - 1)
      task       = lastNode[0]
      argument   = lastNode[1]
      keptNodeID = lastNode[2]

      if keptNodeID == None:
        tool       = self.pipeline.tasks[task]
        attributes = self.nodeMethods.buildNodeFromToolConfiguration(self.tools, tool, argument)

        # Check if the argument is required or not.  Only required nodes are built here.
        keptNodeID = str('OPTION_') + str(self.nodeMethods.optionNodeID)
        self.nodeMethods.optionNodeID += 1
        graph.add_node(keptNodeID, attributes = attributes)

      # Store the ID of the node being kept along with the value it was given in the common nodes
      # section of the configuration file.  The instances information will refer to the common
      # node value and this needs to point to the nodeID in the graph.
      self.nodeIDs[nodeName] = keptNodeID

      # Loop over the remaining nodes, delete them and include edges to the retained nodes.
      #for ID, values in sortedExistingNodes:
      for values in sortedExistingNodes:
        task       = values[0]
        argument   = values[1]
        nodeID     = values[2]
        tool       = self.pipeline.tasks[task]
        if nodeID != None: shortForm  = self.edgeMethods.getEdgeAttribute(graph, nodeID, task, 'shortForm')
        else: shortForm = self.tools.getArgumentData(tool, argument, 'short form argument')

        # Add an edge from the common node to this task.
        attributes           = edgeAttributes()
        attributes.argument  = argument
        attributes.shortForm = shortForm
        graph.add_edge(keptNodeID, task, attributes = attributes)

        # If this option node has associated file nodes, delete them and add edges to the file
        # nodes associated with the kept node.
        isInput  = self.tools.getArgumentData(tool, argument, 'input')
        isOutput = self.tools.getArgumentData(tool, argument, 'output')
        if nodeID != None:

          # Check how many file nodes are associated with the option node being deleted.  If there is more than
          # one associated file node, determine the correct course of action.
          numberOfAssociatedFileNodes = len(self.nodeMethods.getGraphNodeAttribute(graph, nodeID, 'associatedFileNodes'))
          print('TEST', task, argument, numberOfAssociatedFileNodes)
          #if numberOfAssociatedFileNodes > 1:

            # By default,
          print('SORTING FILE NODES', task, argument, self.nodeMethods.getGraphNodeAttribute(graph, nodeID, 'associatedFileNodes'), numberOfAssociatedFileNodes)
          for fileNodeID in self.nodeMethods.getGraphNodeAttribute(graph, nodeID, 'associatedFileNodes'):
            print(fileNodeID, self.nodeMethods.getGraphNodeAttribute(graph, fileNodeID, 'values'))
          exit(0)

            # TODO ADD ALTERNATIVE STRATEGIES.

          # If there was only one file node, delete it and add the new edge.
            #fileNodeID = self.nodeMethods.getGraphNodeAttribute(graph, nodeID, 'associatedFileNodes')[0]
            #graph.remove_node(fileNodeID)
            #if isInput: graph.add_edge(fileNodeID, task, attributes = attributes)
            #elif isOutput: graph.add_edge(task, fileNodeID, attributes = attributes)

        # Remove the node.  First check if this node is associated with a pipeline argument.  If so, reassign
        # the pipeline argument to the keptNodeID.
        if nodeID != None:
          for pipelineArgument in self.pipeline.argumentData:
            pipelineArgumentNodeID = self.pipeline.argumentData[pipelineArgument].nodeID
            if pipelineArgumentNodeID == nodeID: self.pipeline.argumentData[pipelineArgument].nodeID = keptNodeID
          graph.remove_node(nodeID)

  # Parse through the 'nodes' section of the pipeline configuration file and identify which nodes can be
  # removed (i.e. merged with another node).  The nodes to be removed are tagged as to be removed and the
  # node that will replace them is also stored.
  def identifyNodesToRemove(self, graph):
    missingNodeID = 1

    # Create a dictionary to store the tasks and arguments required to build edges from the
    # merged node.  
    edgesToCreate = {}

    for nodeName in self.pipeline.nodeTaskInformation:

      # If there is only a single node, listed, there is no need to proceed, since no merhing needs to
      # take place. 
      optionsToMerge = self.pipeline.nodeTaskInformation[nodeName]
      if len(optionsToMerge) != 1:

        # Pick one of the nodes to keep.  If the option picked has not yet been set as a node, choose
        # again.
        nodeID           = None
        absentNodeValues = []
        while nodeID == None and optionsToMerge:
          optionToKeep = optionsToMerge.pop(0)
          task         = optionToKeep[0]
          argument     = optionToKeep[1]
          nodeID       = self.nodeMethods.doesNodeExist(graph, task, argument)

          # If the node doesn't exist, store the task and argument.  These will be put into the
          # edgesToCreate dictionary once a nodeID has been found.
          if nodeID == None: absentNodeValues.append((task, argument))

        # If none of the nodes exist, a node needs to be created.  For now, store the edges that
        # need to be created
        if nodeID == None:
          tempNodeID                = 'CREATE_NODE_' + str(missingNodeID)
          edgesToCreate[tempNodeID] = []
          missingNodeID += 1
          for task, argument in absentNodeValues: edgesToCreate[tempNodeID].append((None, task, argument))

        # Mark the remaining nodes for deletion and also the nodeID of the node which this node is
        # to be merged with.
        else:

          # If this nodeID already exists in the edgesToCreate dictionary, throw an error.
          # TODO SORT ERROR
          if nodeID in edgesToCreate:
            print('ERROR WITH SHARED NODES.')
            self.errors.terminate()

          # Initialise the entry for this nodeID and add any edges that are stored in the absentNodeValues list.
          edgesToCreate[nodeID] = []
          for task, argument in absentNodeValues: edgesToCreate[nodeID].append((None, task, argument))

          # Now parse through the nodes remaining in the optionsToMerge structure and mark nodes and store edge
          # information.
          for task, argument in optionsToMerge:
            nodeIDToRemove = self.nodeMethods.doesNodeExist(graph, task, argument)

            # Store the task and arguments for those nodes that are to be deleted or do not exist.
            # These will be needed to build edges to the merged nodes.
            edgesToCreate[nodeID].append((nodeIDToRemove, task, argument))

            # Only mark nodes that exist.
            if nodeIDToRemove != None: self.nodeMethods.setGraphNodeAttribute(graph, nodeIDToRemove, 'isMarkedForRemoval', True)

    return edgesToCreate

  # Generate the task workflow from the topologically sorted pipeline graph.
  def generateWorkflow(self, graph):
    workflow  = []
    topolSort = nx.topological_sort(graph)
    for nodeID in topolSort:
      nodeType = self.nodeMethods.getGraphNodeAttribute(graph, nodeID, 'nodeType')
      if nodeType == 'task': workflow.append(nodeID)

    return workflow

  # Check that all defined parameters are valid.
  def checkParameters(self, graph):
    print('***NEED TO CHECK PARAMETERS')

  # Determine all of the outputs from the graph.  This is essentially all file nodes with no successors.
  def determineGraphDependencies(self, graph, key):
    dependencies = []
    fileNodeIDs  = self.nodeMethods.getNodes(graph, 'file')
    for nodeID in fileNodeIDs:

      # Determine if the node has any predecessors.
      hasPredecessor = self.nodeMethods.hasPredecessor(graph, nodeID)

      # If there are no predecessors, find the name of the file and add to the list of dependencies.
      # Since the values associated with a node is a dictionary of lists, if 'key' is set to 'all',
      # get all of the values, otherwise, just get those with the specified key.
      if not hasPredecessor:
        values = self.nodeMethods.getGraphNodeAttribute(graph, nodeID, 'values')
        if key == 'all':
          for iteration in values.keys():
            for value in values[iteration]: dependencies.append(value)

        # Just get values for a particular key.
        elif key in values:
          for value in values[key]: dependencies.append(value)

        # If the key is unknown, fail.
        #TODO Errors.
        else:
          print('UNKNOWN KEY: configurationClass.determineGraphDependencies', key)
          self.errors.terminate()

    return dependencies

  # Determine all of the dependencies in the graph.  This is essentially all file nodes with no predecessors.
  def determineGraphOutputs(self, graph, key):
    outputs     = []
    fileNodeIDs = self.nodeMethods.getNodes(graph, 'file')
    for nodeID in fileNodeIDs:

      # Determine if the node has any successors.
      hasSuccessor = self.nodeMethods.hasSuccessor(graph, nodeID)

      # If there are no predecessors, find the name of the file and add to the list of dependencies.
      # Since the values associated with a node is a dictionary of lists, if 'key' is set to 'all',
      # get all of the values, otherwise, just get those with the specified key.
      if not hasSuccessor:
        values = self.nodeMethods.getGraphNodeAttribute(graph, nodeID, 'values')
        if key == 'all':
          for iteration in values.keys():
            for value in values[iteration]: outputs.append(value)

        # Just get values for a particular key.
        elif key in values:
          for value in values[key]: outputs.append(value)

        # If the key is unknown, fail.
        #TODO Errors.
        else:
          print('UNKNOWN KEY: configurationClass.determineGraphOutputs', key)
          self.errors.terminate()

    return outputs

  # Determine all of the intermediate files in the graph.  This is all of the file nodes that have both
  # predecessor and successor nodes.
  def determineGraphIntermediateFiles(self, graph, key):
    intermediates = []
    fileNodeIDs   = self.nodeMethods.getNodes(graph, 'file')
    for nodeID in fileNodeIDs:

      # Determine if the node has any predecessors or successors.
      hasPredecessor = self.nodeMethods.hasPredecessor(graph, nodeID)
      hasSuccessor   = self.nodeMethods.hasSuccessor(graph, nodeID)

      if hasPredecessor and hasSuccessor:
        values = self.nodeMethods.getGraphNodeAttribute(graph, nodeID, 'values')
        if key == 'all':
          for iteration in values.keys():
            for value in values[iteration]: intermediates.append(value)

        # Just get values for a particular key.
        elif key in values:
          for value in values[key]: intermediates.append(value)

        # If the key is unknown, fail.
        #TODO Errors.
        else:
          print('UNKNOWN KEY: configurationClass.determineGraphIntermediateFiles', key)
          self.errors.terminate()

    return intermediates

  # Check all of tha provided information.
  def checkData(self, graph):
    optionNodes = self.nodeMethods.getNodes(graph, 'option')
    for optionNodeID in optionNodes:

      # Check if there are any values associated with this node.
      values = self.nodeMethods.getGraphNodeAttribute(graph, optionNodeID, 'values')
      if values:

        for iteration in values:

          # First check to see if multiple values have been given erroneously.
          numberOfValues      = len(values[iteration])
          allowMultipleValues = self.nodeMethods.getGraphNodeAttribute(graph, optionNodeID, 'allowMultipleValues')

          #TODO SORT OUT ERRORS.
          if not allowMultipleValues and numberOfValues != 1:
            print('GIVEN MULTIPLE VALUES WHEN NOT ALLOWED', values[iteration])
            self.errors.terminate()

          # Determine the expected data type
          expectedDataType = self.nodeMethods.getGraphNodeAttribute(graph, optionNodeID, 'dataType')
          for value in values[iteration]:

            # Get the data type for the value and check that it is as expected.
            if not self.checkDataType(expectedDataType, value):
              #TODO SORT ERROR.
              print('Unexpected data type:', value, expectedDataType)
              self.errors.terminate()

            # If the value refers to a file, check that the extension is valid.
            if self.nodeMethods.getGraphNodeAttribute(graph, optionNodeID, 'isFile'):
              if not self.checkFileExtension(graph, optionNodeID, value):
                #TODO SORT ERROR
                print('Wrong file extension', value)
                self.errors.terminate()

  # Check if data types agree.
  def checkDataType(self, expectedType, value):
    success = True
  
    # Check that flags have the value "set" or "unset".
    if expectedType == 'flag':
      if value != 'set' and value != 'unset': success = False
  
    # Boolean values should be set to 'true', 'True', 'false' or 'False'.
    elif expectedType == 'bool':
      if value != 'true' and value != 'True' and value != 'false' and value != 'False': success = False
  
    # Check integers...
    elif expectedType == 'integer':
      try: test = int(value)
      except: success = False
  
    # Check floats...
    elif expectedType == 'float':
      try: test = float(value)
      except: success = False
  
    # and strings.
    elif expectedType == 'string':
      try: test = str(value)
      except: success = False
  
    # If the data type is unknown.
    else:
      success = False
  
    return success

  #TODO FINISH
  # Check that a file extension is valid.
  def checkFileExtension(self, graph, nodeID, value):
    extensions = self.nodeMethods.getGraphNodeAttribute(graph, nodeID, 'allowedExtensions')
    for extension in extensions:
      if value.endswith(extension):
        return True

    return False
