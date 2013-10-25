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

import graphPlotting
from graphPlotting import *

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
    self.drawing           = drawGraph()
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

    # Before creating all of the new edges, find any nodes that have not been created but were called
    # on to be merged.  Create the nodes and update the edgesToCreate structure with the new ID.
    self.createMissingMergedNodes(graph, edgesToCreate)

    # Parse through all of the edges that need to be created.  Create nodes where necessary and handle
    # cases where the option is a filename stub and there are multiple file nodes to handle.
    self.createEdgesForMergedNodes(graph, edgesToCreate)

    #
    self.createEdgesForMergedFileNodes(graph, edgesToCreate)

    # Having completed the merging process, purge the nodes that are no longer required.
    self.nodeMethods.purgeNodeMarkedForRemoval(graph)
    self.drawing.drawDot(graph, self.nodeMethods, self.edgeMethods, 'beforePurge.dot')
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

  # Create missing merged nodes.
  def createMissingMergedNodes(self, graph, edgesToCreate):
    createdNodes = {}
    for mergeNodeID in edgesToCreate:
      for nodeID, task, argument in edgesToCreate[mergeNodeID]:

        # If the node does not exist (i.e. none of the nodes being merged had been added to the graph),
        # the ID will begin with 'CREATE_NODE'.  If this is the case, create the node.
        if mergeNodeID.startswith('CREATE_NODE') and mergeNodeID not in createdNodes:
          tempNodeID = 'OPTION_' + str(self.nodeMethods.optionNodeID)
          self.nodeMethods.optionNodeID += 1
          tool       = self.pipeline.tasks[task]
          attributes = self.nodeMethods.buildNodeFromToolConfiguration(self.tools, tool, argument)
          graph.add_node(tempNodeID, attributes = attributes)

          # With the node addded, add the mergeNodeID to the dictionary containing nodes created in
          # this routine.  There will be at least two edges required for any of the nodes to be
          # created, but the node only needs to be created once.
          createdNodes[mergeNodeID] = tempNodeID

    # Having created all of the necessary nodes, update the edgesToCreate structure to include the new
    # IDs.
    for nodeID in createdNodes: edgesToCreate[createdNodes[nodeID]] = edgesToCreate.pop(nodeID)
 
  # For each node that is removed in the merging process, edges need to be created from the merged node
  # to the task whose original node has been merged.
  def createEdgesForMergedNodes(self, graph, edgesToCreate):
    for mergeNodeID in edgesToCreate:
      for nodeID, task, argument in edgesToCreate[mergeNodeID]:
        tool = self.pipeline.tasks[task]

        # Find the short and long form of the argument.
        longFormArgument  = self.tools.getLongFormArgument(tool, argument)
        shortFormArgument = self.tools.getArgumentData(tool, longFormArgument, 'short form argument')

        # Add an edge from the merged node to this task.
        attributes           = edgeAttributes()
        attributes.argument  = longFormArgument
        attributes.shortForm = shortFormArgument
        graph.add_edge(mergeNodeID, task, attributes = attributes)

  # Create edges from the merged file nodes to the tasks whose own file nodes were marked
  # for removal in the merging process.  Filename stubs have to be handled here.
  def createEdgesForMergedFileNodes(self, graph, edgesToCreate):
    for mergeNodeID in edgesToCreate:
      for nodeID, task, argument in edgesToCreate[mergeNodeID]:

        # If the nodeID exists, then option node for this task already exists in the graph and
        # has been marked for removal.  The associated file nodes will therefore, also exist and
        # so these should also be marked for removal.
        if nodeID:
          fileNodeIDs = self.nodeMethods.getGraphNodeAttribute(graph, nodeID, 'associatedFileNodes')
          for fileNodeID in fileNodeIDs: self.nodeMethods.setGraphNodeAttribute(graph, fileNodeID, 'isMarkedForRemoval', True)

        # Only look at options nodes that contain information about files.
        if self.nodeMethods.getGraphNodeAttribute(graph, mergeNodeID, 'isFile'):
          tool                      = self.pipeline.tasks[task]
          mergedNodeisFilenameStub  = self.nodeMethods.getGraphNodeAttribute(graph, mergeNodeID, 'isFilenameStub')
          removedNodeisFilenameStub = self.tools.getArgumentData(tool, argument, 'is filename stub')
          if removedNodeisFilenameStub == None: removedNodeisFilenameStub = False

          # Find the short and long form of the argument.
          longFormArgument     = self.tools.getLongFormArgument(tool, argument)
          shortFormArgument    = self.tools.getArgumentData(tool, longFormArgument, 'short form argument')
          isInput              = self.tools.getArgumentData(tool, longFormArgument, 'input')
          isOutput             = self.tools.getArgumentData(tool, longFormArgument, 'output')

          # If the argument is not for a filename stub, then there is a single output file.
          # Generate the edges from the replacement file value to this task.
          if not mergedNodeisFilenameStub and not removedNodeisFilenameStub:
            self.linkNonFilenameStubNodes(graph, mergeNodeID, nodeID, task, shortFormArgument, longFormArgument, isInput)

          # If either of the nodes are filename stubs, deal with them.
          # is an input of an output.
          elif mergedNodeisFilenameStub and not removedNodeisFilenameStub:
            self.createFilenameStubEdgesM(graph, mergeNodeID, nodeID, task, shortFormArgument, longFormArgument)
          elif not mergedNodeisFilenameStub and removedNodeisFilenameStub:
            self.createFilenameStubEdgesR(graph, mergeNodeID, nodeID, task, tool, shortFormArgument, longFormArgument)
          elif mergedNodeisFilenameStub and removedNodeisFilenameStub:
            self.createFilenameStubEdgesMR(graph, mergeNodeID, nodeID, task, shortFormArgument, longFormArgument, isInput)

  # Create the edges for file nodes that are not generated from filename stubs.
  def linkNonFilenameStubNodes(self, graph, mergeNodeID, nodeID, task, shortFormArgument, longFormArgument, isInput):

    # Find the file nodes associated with the option node.
    mergeFileNodeIDs = self.nodeMethods.getGraphNodeAttribute(graph, mergeNodeID, 'associatedFileNodes')
    fileNodeIDs      = self.nodeMethods.getGraphNodeAttribute(graph, nodeID, 'associatedFileNodes')
    if len(mergeFileNodeIDs) != 1 or len(fileNodeIDs) != 1:
      #TODO SORT ERROR.
      print('UNEXPECTED NUMBER OF FILENODE IDs - createEdgesForMergedFileNodes')
      self.errors.terminate()

    attributes           = edgeAttributes()
    attributes.argument  = longFormArgument
    attributes.shortForm = shortFormArgument
    if isInput: graph.add_edge(mergeFileNodeIDs[0], task, attributes = attributes)
    else: graph.add_edge(task, mergeFileNodeIDs[0], attributes = attributes)

  # TODO WRITE THIS ROUTINE.
  # Create the edges for file nodes that are generated from filename stubs.  Specifically, deal
  # with the case where the node being kept is a filename stub and the node being removed is not.
  def createFilenameStubEdgesM(self, graph, mergeNodeID, nodeID, task, shortFormArgument, longFormArgument):
    print('HAVE NOT IMPLEMENTED YET.')
    print('The node being removed is not a filename stub, but the one remaining is.')
    self.errors.terminate()

  # Create the edges for file nodes that are generated from filename stubs.  Specifically, deal
  # with the case where the node being kept is not a filename stub and the node being removed is.
  def createFilenameStubEdgesR(self, graph, mergeNodeID, nodeID, task, tool, shortFormArgument, longFormArgument):
    fileNodeIDs = []

    # Since the node being kept is not a filename stub, it will only have a single file node associated
    # with it. Reiname this file node (with the suffix '_1') and create the correct number of file nodes.
    mergeFileNodeIDs = self.nodeMethods.getGraphNodeAttribute(graph, mergeNodeID, 'associatedFileNodes')
    if len(mergeFileNodeIDs) != 1:
      #TODO SORT ERROR.
      print('UNEXPECTED NUMBER OF FILENODE IDs - createFilenameStubEdgesR')
      self.errors.terminate()

    # Rename the existing file node.
    self.nodeMethods.renameNode(graph, mergeFileNodeIDs[0], mergeFileNodeIDs[0] + '_1')
    fileNodeIDs.append(mergeFileNodeIDs[0] + '_1')

    # Create the additional file nodes.
    outputs    = self.tools.getArgumentData(tool, longFormArgument, 'filename extensions')
    for count in range(2, len(outputs) + 1):
      fileNodeID                     = mergeNodeID + '_FILE_' + str(count)
      attributes                     = fileNodeAttributes()
      attributes.description         = self.nodeMethods.getGraphNodeAttribute(graph, mergeNodeID, 'description')
      attributes.allowMultipleValues = self.nodeMethods.getGraphNodeAttribute(graph, mergeNodeID, 'allowMultipleValues')
      attributes.allowedExtensions   = self.nodeMethods.getGraphNodeAttribute(graph, mergeNodeID, 'allowedExtensions')
      fileNodeIDs.append(fileNodeID)
      graph.add_node(fileNodeID, attributes = attributes)

    # Create edges from all of the file nodes to the task associated with the node being removed.
    for fileNodeID in fileNodeIDs:
      attributes           = edgeAttributes()
      attributes.argument  = longFormArgument
      attributes.shortForm = shortFormArgument
      if self.tools.getArgumentData(tool, longFormArgument, 'input'): graph.add_edge(fileNodeID, task, attributes = attributes)
      else: graph.add_edge(task, fileNodeID, attributes = attributes)

    #TODO WHAT IF WE WANT ALL OF THE OUTPUTS FROM THE TOOL WITH THE FILENAME STUB TO GO TO THE NEXT NODE?

  # Create the edges for file nodes that are generated from filename stubs.  Specifically, deal
  # with the case where both the node being kept and the node being removed are filename stubs.
  # This is the simplest case, as no choices are required, just include edges from all the file
  # nodes to the task.
  def createFilenameStubEdgesMR(self, graph, mergeNodeID, nodeID, task, shortFormArgument, longFormArgument, isInput):
    mergeFileNodeIDs = self.nodeMethods.getGraphNodeAttribute(graph, mergeNodeID, 'associatedFileNodes')
    for mergeFileNodeID in mergeFileNodeIDs:
      attributes           = edgeAttributes()
      attributes.argument  = longFormArgument
      attributes.shortForm = shortFormArgument
      if isInput: graph.add_edge(mergeFileNodeID, task, attributes = attributes)
      else: graph.add_edge(task, mergeFileNodeID, attributes = attributes)

  # Generate the task workflow from the topologically sorted pipeline graph.
  def generateWorkflow(self, graph):
    workflow  = []
    topolSort = nx.topological_sort(graph)
    for nodeID in topolSort:
      if self.nodeMethods.getGraphNodeAttribute(graph, nodeID, 'nodeType') == 'task': workflow.append(nodeID)

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
