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

import instances
from instances import *

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

class configurationMethods:
  def __init__(self):

    # Define methods for handling nodes and edges.
    self.edgeMethods = edgeClass()
    self.nodeMethods = nodeClass()

    # Define a class for handling instances.
    self.instances = instanceConfiguration()

    # Define the errors class.
    self.errors = configurationClassErrors()

    # Define operations for operating on files.
    self.fileOperations = fileOperations()

    # Define a class for handling pipelines.
    self.pipeline = pipelineConfiguration()

    # Define a class for handling tools.
    self.tools = toolConfiguration()

    # Define methods for plotting the pipeline graph.
    self.drawing = drawGraph()

    self.nodeIDs = {}

  # Build a graph for an individual task.  The pipeline is built by merging nodes between
  # different tasks.  This step is performed later.
  def buildTaskGraph(self, graph, tasks):
    for task in tasks:
      tool = self.pipeline.taskAttributes[task].tool

      # Generate the task node.
      self.nodeMethods.buildTaskNode(graph, self.tools, self.pipeline, task, tool)

      # Find all required arguments for this task and build nodes for them all.  Link these nodes
      # to the task node.
      self.nodeMethods.buildRequiredPredecessorNodes(graph, self.tools, task)

      # TODO ENSURE THAT ADDITIONAL FILES, E.G. STUBS AND INDEXES ARE INCLUDED.

  # Assign values from the nodes section of the pipeline configuration file to the nodes.
  def assignPipelineAttributes(self, graph, tasks):

    # For nodes in the pipeline configuration file, find any that have the extension field.
    for nodeName in self.pipeline.linkedExtension:
      for task, argument in self.pipeline.commonNodes[nodeName]:
        nodeID = self.nodeMethods.getNodeForTaskArgument(graph, task, argument, 'option')[0]
        self.nodeMethods.setGraphNodeAttribute(graph, nodeID, 'linkedExtension', self.pipeline.linkedExtension[nodeName])

  # If the pipeline configuration file links a filename stub argument from one task to a non-filename
  # stub argument in another, the explicit extension must be included in the configuration file. Check
  # that this is the case.
  def checkStubConnections(self, graph):

    # Loop over all of the nodes in the commonNodes structure. This includes all of the nodes defined
    # in the pipeline configuration file.
    for nodeID in self.pipeline.commonNodes:
      nonStubArguments = []
      stubArguments    = []

      # Get the task/argument pairs associated with the node.
      for task, argument in self.pipeline.commonNodes[nodeID]:
        tool = self.nodeMethods.getGraphNodeAttribute(graph, task, 'tool')

        # Determine if this task argument is a filename stub.
        if self.tools.getArgumentAttribute(tool, argument, 'isFilenameStub'): stubArguments.append((task, argument))
        else: nonStubArguments.append((task, argument))

      # If the node has both a stub and non-stub argument associated with it, ensure that the pipeline
      # configuration file for this node contains the extension attribute.
      #if nonStubArguments and stubArguments and not self.pipeline.nodeAttributes[nodeID].extension:
      #  self.errors.missingExtensionForNonStub(nodeID, stubArguments, nonStubArguments)

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

    # Now add edges for the file nodes.
    self.createEdgesForMergedFileNodes(graph, edgesToCreate)

    # Having completed the merging process, purge the nodes that are no longer required.
    self.nodeMethods.purgeNodeMarkedForRemoval(graph)

    # Parse through all of the nodes that have been merged and check if they have files that
    # were marked in the pipeline configuration file as files that should be kept. If such
    # nodes exist, mark them.
    self.markNodesWithFilesToBeKept(graph)

    # Mark all edges that are greedy. If the input to a particular task is several sets of data, there
    # are two possible ways to handle this. The default is that the task will be run multiple times for
    # each set of data. However, if the task argument accepting the multiple sets of data is listed as
    # being 'greedy', all of the iterations will be collapsed into a single input and all of the files
    # will be used as input to a single run of the task.
    self.markGreedyEdges(graph)

  # Parse through the 'nodes' section of the pipeline configuration file and identify which nodes can be
  # removed (i.e. merged with another node).  The nodes to be removed are tagged as to be removed and the
  # node that will replace them is also stored.
  def identifyNodesToRemove(self, graph):
    missingNodeID = 1

    # Create a dictionary to store the tasks and arguments required to build edges from the
    # merged node.  
    edgesToCreate = {}
    for configNodeID in self.pipeline.commonNodes:

      # Check if there are files associated with this node that should be kept (i.e. not marked as
      # intermediate files.)
      keepFiles = self.pipeline.nodeAttributes[configNodeID].keepFiles

      # If there is only a single node listed, there is no need to proceed, since no merging needs to
      # take place. 
      optionsToMerge = self.pipeline.commonNodes[configNodeID]
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

          # Store the ID of the node being kept along with the value it was given in the common nodes
          # section of the configuration file.  The instances information will refer to the common
          # node value and this needs to point to the nodeID in the graph.
          self.nodeIDs[configNodeID] = nodeID

          # If the node doesn't exist, store the task and argument.  These will be put into the
          # edgesToCreate dictionary once a nodeID has been found.
          if nodeID == None: absentNodeValues.append((task, argument))

        # If none of the nodes exist, a node needs to be created.  For now, store the edges that
        # need to be created
        if nodeID == None:
          tempNodeID                 = 'CREATE_NODE_' + str(missingNodeID)
          edgesToCreate[tempNodeID]  = []
          self.nodeIDs[configNodeID] = tempNodeID
          missingNodeID += 1
          for task, argument in absentNodeValues: edgesToCreate[tempNodeID].append((None, task, argument))

        # Mark the remaining nodes for deletion and also the nodeID of the node which this node is
        # to be merged with.
        else:

          # Initialise the dictionary if this nodeID is not present.
          if nodeID not in edgesToCreate: edgesToCreate[nodeID] = []

          # Initialise the entry for this nodeID and add any edges that are stored in the absentNodeValues list.
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
          tool       = self.nodeMethods.getGraphNodeAttribute(graph, task, 'tool')
          attributes = self.nodeMethods.buildNodeFromToolConfiguration(self.tools, tool, argument)
          graph.add_node(tempNodeID, attributes = attributes)

          # With the node addded, add the mergeNodeID to the dictionary containing nodes created in
          # this routine.  There will be at least two edges required for any of the nodes to be
          # created, but the node only needs to be created once.
          createdNodes[mergeNodeID] = tempNodeID

          # Modify the value in the nodeIDs dictionary to reflect this modified node value.
          for storedNodeID in self.nodeIDs:
            if mergeNodeID == self.nodeIDs[storedNodeID]: self.nodeIDs[storedNodeID] = tempNodeID

    # Having created all of the necessary nodes, update the edgesToCreate structure to include the new
    # IDs.
    for nodeID in createdNodes: edgesToCreate[createdNodes[nodeID]] = edgesToCreate.pop(nodeID)
 
  # For each node that is removed in the merging process, edges need to be created from the merged node
  # to the task whose original node has been merged.
  def createEdgesForMergedNodes(self, graph, edgesToCreate):
    for mergeNodeID in edgesToCreate:
      for nodeID, task, argument in edgesToCreate[mergeNodeID]:
        tool = self.nodeMethods.getGraphNodeAttribute(graph, task, 'tool')

        # Add an edge from the merged node to this task.
        attributes = self.edgeMethods.addEdge(graph, self.nodeMethods, self.tools, mergeNodeID, task, argument)

  # Create edges from the merged file nodes to the tasks whose own file nodes were marked
  # for removal in the merging process.  Filename stubs have to be handled here.
  def createEdgesForMergedFileNodes(self, graph, edgesToCreate):
    for mergeNodeID in edgesToCreate:
      for nodeID, task, argument in edgesToCreate[mergeNodeID]:

        # If the nodeID exists, then an option node for this task already exists in the graph and
        # has been marked for removal.  The associated file nodes will therefore, also exist and
        # so these should also be marked for removal.
        if nodeID:
          for fileNodeID in self.nodeMethods.getGraphNodeAttribute(graph, nodeID, 'associatedFileNodes'):
            self.nodeMethods.setGraphNodeAttribute(graph, fileNodeID, 'isMarkedForRemoval', True)

        # Only look at options nodes that contain information about files.
        if self.nodeMethods.getGraphNodeAttribute(graph, mergeNodeID, 'isFile'):
          tool                      = self.nodeMethods.getGraphNodeAttribute(graph, task, 'tool')
          mergedNodeisFilenameStub  = self.nodeMethods.getGraphNodeAttribute(graph, mergeNodeID, 'isFilenameStub')
          removedNodeisFilenameStub = self.tools.getArgumentAttribute(tool, argument, 'isFilenameStub')
          if removedNodeisFilenameStub == None: removedNodeisFilenameStub = False

          # Find the short and long form of the argument.
          longFormArgument     = self.tools.getLongFormArgument(tool, argument)
          shortFormArgument    = self.tools.getArgumentAttribute(tool, longFormArgument, 'shortFormArgument')
          isInput              = self.tools.getArgumentAttribute(tool, longFormArgument, 'isInput')
          isOutput             = self.tools.getArgumentAttribute(tool, longFormArgument, 'isOutput')

          # If the argument is not for a filename stub, then there is a single output file.
          # Generate the edges from the replacement file value to this task.
          if not mergedNodeisFilenameStub and not removedNodeisFilenameStub:
            self.linkNonFilenameStubNodes(graph, mergeNodeID, nodeID, task, shortFormArgument, longFormArgument, isInput)

          # If either of the nodes are filename stubs, deal with them.
          elif mergedNodeisFilenameStub and not removedNodeisFilenameStub:
            self.createFilenameStubEdgesM(graph, mergeNodeID, nodeID, task, shortFormArgument, longFormArgument)
          elif not mergedNodeisFilenameStub and removedNodeisFilenameStub:
            self.createFilenameStubEdgesR(graph, mergeNodeID, nodeID, task, tool, shortFormArgument, longFormArgument)
          elif mergedNodeisFilenameStub and removedNodeisFilenameStub:
            self.createFilenameStubEdgesMR(graph, mergeNodeID, nodeID, task, shortFormArgument, longFormArgument, isInput)

  # Create the edges for file nodes that are not generated from filename stubs.
  def linkNonFilenameStubNodes(self, graph, mergeNodeID, nodeID, task, shortFormArgument, longFormArgument, isInput):

    # Find the file nodes associated with the option node.
    mergeFileNodeIDs = self.nodeMethods.getAssociatedFileNodeIDs(graph, mergeNodeID)

    # If the node has been created, find the associated file node IDs. If the node has not yet been 
    # created in the graph, this is unnecessary.
    if nodeID != None:
      fileNodeIDs      = self.nodeMethods.getAssociatedFileNodeIDs(graph, nodeID)

      if len(mergeFileNodeIDs) != 1 or len(fileNodeIDs) != 1:
        #TODO SORT ERROR.
        print('UNEXPECTED NUMBER OF FILENODE IDs - createEdgesForMergedFileNodes')
        self.errors.terminate()

    tool = self.nodeMethods.getGraphNodeAttribute(graph, task, 'tool')
    if isInput: self.edgeMethods.addEdge(graph, self.nodeMethods, self.tools, mergeFileNodeIDs[0], task, longFormArgument)
    else: self.edgeMethods.addEdge(graph, self.nodeMethods, self.tools, task, mergeFileNodeIDs[0], longFormArgument)

  # Create the edges for file nodes that are generated from filename stubs.  Specifically, deal
  # with the case where the node being kept is a filename stub and the node being removed is not.
  def createFilenameStubEdgesM(self, graph, mergeNodeID, nodeID, task, shortFormArgument, longFormArgument):
    foundMatch = False
    tool       = self.nodeMethods.getGraphNodeAttribute(graph, task, 'tool')

    # The node being kept is a filename stub, so has multiple file nodes associated with it. First
    # loop over these file nodes.
    for fileNodeID in self.nodeMethods.getAssociatedFileNodeIDs(graph, mergeNodeID):
      
      # Get the extension that the file is expecting. Add a '.' to the front of this extension. The extensions
      # supplied for filename stubs begin with a '.'.
      extension = '.' + self.pipeline.getExtension(task, longFormArgument, self.nodeMethods.getGraphNodeAttribute(graph, nodeID, 'linkedExtension'))
      if extension in self.nodeMethods.getGraphNodeAttribute(graph, fileNodeID, 'allowedExtensions'):
        foundMatch = True

        # Create the edge from the file node to the task.
        isInput = self.tools.getArgumentAttribute(tool, longFormArgument, 'isInput')
        if isInput: self.edgeMethods.addEdge(graph, self.nodeMethods, self.tools, fileNodeID, task, longFormArgument)
        else: self.edgeMethods.addEdge(graph, self.nodeMethods, self.tools, task, fileNodeID, longFormArgument)
        break

    # If the expected extension was not available in any of the file nodes, this must be an error in the
    # pipeline configuration file.
    if not foundMatch:
      #TODO ERROR
      print('createFilenameStubEdgesM')
      self.errors.terminate()

  # Create the edges for file nodes that are generated from filename stubs.  Specifically, deal
  # with the case where the node being kept is not a filename stub and the node being removed is.
  def createFilenameStubEdgesR(self, graph, mergeNodeID, nodeID, task, tool, shortFormArgument, longFormArgument):
    fileNodeIDs = []

    # Since the node being kept is not a filename stub, it will only have a single file node associated
    # with it. Rename this file node (with the suffix '_1') and create the correct number of file nodes.
    mergeFileNodeIDs = self.nodeMethods.getGraphNodeAttribute(graph, mergeNodeID, 'associatedFileNodes')
    if len(mergeFileNodeIDs) != 1:
      #TODO SORT ERROR.
      print('UNEXPECTED NUMBER OF FILENODE IDs - createFilenameStubEdgesR')
      self.errors.terminate()

    # Get the extensions for the output files.
    outputExtensions = self.tools.getArgumentAttribute(tool, longFormArgument, 'filenameExtensions')

    # Rename the existing file node and reset the extension.
    self.nodeMethods.renameNode(graph, self.tools, mergeFileNodeIDs[0], mergeFileNodeIDs[0] + '_1')
    fileNodeIDs.append(mergeFileNodeIDs[0] + '_1')

    # Create the additional file nodes.
    for count in range(2, len(outputExtensions) + 1):
      extension                      = outputExtensions[count - 1]
      fileNodeID                     = mergeNodeID + '_FILE_' + str(count)
      attributes                     = fileNodeAttributes()
      attributes.description         = self.nodeMethods.getGraphNodeAttribute(graph, mergeNodeID, 'description')
      attributes.allowMultipleValues = self.nodeMethods.getGraphNodeAttribute(graph, mergeNodeID, 'allowMultipleValues')
      attributes.allowedExtensions   = [extension]
      fileNodeIDs.append(fileNodeID)
      graph.add_node(fileNodeID, attributes = attributes)

    # Create edges from all of the file nodes to the task associated with the node being removed.
    for fileNodeID in fileNodeIDs:
      isInput = self.tools.getArgumentAttribute(tool, longFormArgument, 'isInput')
      if isInput: self.edgeMethods.addEdge(graph, self.nodeMethods, self.tools, fileNodeID, task, longFormArgument)
      else: self.edgeMethods.addEdge(graph, self.nodeMethods, self.tools, task, fileNodeID, longFormArgument)

    #TODO WHAT IF WE WANT ALL OF THE OUTPUTS FROM THE TOOL WITH THE FILENAME STUB TO GO TO THE NEXT NODE?

  # Create the edges for file nodes that are generated from filename stubs.  Specifically, deal
  # with the case where both the node being kept and the node being removed are filename stubs.
  # This is the simplest case, as no choices are required, just include edges from all the file
  # nodes to the task.
  def createFilenameStubEdgesMR(self, graph, mergeNodeID, nodeID, task, shortFormArgument, longFormArgument, isInput):
    mergeFileNodeIDs = self.nodeMethods.getGraphNodeAttribute(graph, mergeNodeID, 'associatedFileNodes')
    for mergeFileNodeID in mergeFileNodeIDs:
      tool = self.nodeMethods.getGraphNodeAttribute(graph, task, 'tool')
      if isInput: self.edgeMethods.addEdge(graph, self.nodeMethods, self.tools, mergeFileNodeID, task, longFormArgument)
      else: self.edgeMethods.addEdge(graph, self.nodeMethods, self.tools, task, mergeFileNodeID, longFormArgument)

  # Parse through all of the nodes that have been merged and check if they have files that
  # were marked in the pipeline configuration file as files that should be kept. If such
  # nodes exist, mark them.
  def markNodesWithFilesToBeKept(self, graph):
    for configNodeID in self.nodeIDs:
      nodeID = self.nodeIDs[configNodeID]
      if self.pipeline.nodeAttributes[configNodeID].keepFiles: self.nodeMethods.setGraphNodeAttribute(graph, nodeID, 'keepFiles', True)

  # Mark all greedy edges in the graph.
  def markGreedyEdges(self, graph):

    # Loop over all of the tasks.
    for task in self.pipeline.greedyTasks:
      for argument in self.pipeline.greedyTasks[task]:
        for sourceNodeID in self.nodeMethods.getNodeForTaskArgument(graph, task, argument, 'option'):
          self.edgeMethods.setEdgeAttribute(graph, sourceNodeID, task, 'isGreedy', True)

  # Check to see if there are any isolated nodes in the pipeline.
  def checkForIsolatedNodes(self, graph):
    isolatedNodes = []
    isIsolated    = False

    for task in self.pipeline.workflow:
      isTaskIsolated = True

      # Check if any of the files being used as input are used by any other tasks in the pipeline.
      for fileNodeID in self.nodeMethods.getPredecessorFileNodes(graph, task):

        # Check if the file is used by any other tasks,
        if graph.predecessors(fileNodeID) or len(graph.successors(fileNodeID)) > 1: isTaskIsolated = False

      # Now check if any files output by the task are used by other tasks.
      for fileNodeID in self.nodeMethods.getSuccessorFileNodes(graph, task):
        if graph.successors(fileNodeID): isTaskIsolated = False

      if isTaskIsolated:
        isolatedNodes.append(task)
        isIsolated = True

    return isIsolated, isolatedNodes

  # Generate the task workflow from the topologically sorted pipeline graph.
  def generateWorkflow(self, graph):
    workflow  = []
    for nodeID in nx.topological_sort(graph):
      if self.nodeMethods.getGraphNodeAttribute(graph, nodeID, 'nodeType') == 'task': workflow.append(nodeID)

    return workflow

  # Attach the instance arguments to the relevant nodes.
  def attachPipelineInstanceArgumentsToNodes(self, graph, pipelineName, instanceName):
    for node in self.instances.instanceAttributes[pipelineName][instanceName].nodes:

      # Get the long form of the argument.
      longFormArgument = self.pipeline.getLongFormArgument(graph, node.argument)
      configNodeID     = self.pipeline.pipelineArguments[longFormArgument].configNodeID

      # Check all of the available pipeline arguments and find the node ID of the node which
      # this instance node is trying to set.
      try: nodeIDToSet = self.nodeIDs[configNodeID]

      # If the configuration node ID is not present in the nodeIDs data structure, it may be
      # because no graph nodes were merged (the nodeIDs data structure is built up when merging
      # graph nodes). In this case, take one of the task/argument pairs associated with this
      # configuration file node and determine the graph node to which this points.
      except:
        task, argument = self.pipeline.commonNodes[configNodeID][0]
        try: nodeIDToSet = self.nodeMethods.getNodeForTaskArgument(graph, task, argument, 'option')[0]
        except:
          #TODO ERROR
          print('WOOPS - configurationData.attachPipelineInstanceArgumentsToNodes')
          self.errors.terminate()

      # All of the values extracted from the instance json file are unicode.  Convert them to strings.
      for counter, value in enumerate(node.values): node.values[counter] = str(value)
      self.nodeMethods.addValuesToGraphNode(graph, nodeIDToSet, node.values, write = 'replace')

  # Attach the instance arguments to the relevant nodes.
  def attachToolInstanceArgumentsToNodes(self, graph, tool, instance):
    for node in self.instances.instanceAttributes[tool][instance].nodes:
      nodeIDToSet = None

      # Check all of the nodes set for this tool, determine the associated arguments and find the node ID
      # of the node for the argument set in the instance.
      for nodeID in self.nodeMethods.getPredecessorOptionNodes(graph, tool):
        argument = self.edgeMethods.getEdgeAttribute(graph, nodeID, tool, 'longFormArgument')
        if argument == node.argument: nodeIDToSet = nodeID

      # If the node doesn't exist, check that the argument requested in the instance is valid for this tool.
      if not nodeIDToSet:
        if node.argument not in self.tools.longFormArguments[tool] and node.argument not in self.tools.shortFormArguments[tool]:
          self.errors.invalidArgumentInToolInstance(tool, instance, node.ID, node.argument)

        # Get the long form of the argument to be set.
        longFormArgument  = self.tools.getLongFormArgument(tool, node.argument)

        # Define the node attributes using information from the tool configuration file.
        attributes = self.nodeMethods.buildNodeFromToolConfiguration(self.tools, tool, longFormArgument)

        # Create a new node for the argument.
        nodeIDToSet = 'OPTION_' + str(self.nodeMethods.optionNodeID)
        self.nodeMethods.optionNodeID += 1
        graph.add_node(nodeIDToSet, attributes = attributes)

        # Add an edge from the new node to the tool node.
        self.edgeMethods.addEdge(graph, self.nodeMethods, self.tools, nodeIDToSet, tool, longFormArgument)

        # If the option node corresponds to a file, build a file node.
        if self.nodeMethods.getGraphNodeAttribute(graph, nodeIDToSet, 'isFile'):
          shortFormArgument = self.edgeMethods.getEdgeAttribute(graph, nodeIDToSet, tool, 'shortFormArgument')
          if self.nodeMethods.getGraphNodeAttribute(graph, nodeIDToSet, 'isInput'):
            self.nodeMethods.buildTaskFileNodes(graph, self.tools, nodeIDToSet, tool, longFormArgument, shortFormArgument, 'input')
          else:
            self.nodeMethods.buildTaskFileNodes(graph, self.tools, nodeIDToSet, tool, longFormArgument, shortFormArgument, 'input')

      # Add the values to the node.
      self.nodeMethods.addValuesToGraphNode(graph, nodeIDToSet, node.values, write = 'replace')

  # Check that all required files and values have been set. All files and parameters that are listed as
  # required by the infividual tools should already have been checked, but if the pipeline has some
  # additional requirements, these may not yet have been checked.
  def checkRequiredFiles(self, graph):

    # Loop over all of the tasks in the pipeline.
    for task in self.pipeline.workflow:

      # Loop over all predecessor file nodes.
      for fileNodeID in self.nodeMethods.getPredecessorFileNodes(graph, task):
        if not self.nodeMethods.getGraphNodeAttribute(graph, fileNodeID, 'values'):

          # Get the long and short form of the argument.
          taskLongFormArgument                                = self.edgeMethods.getEdgeAttribute(graph, fileNodeID, task, 'longFormArgument')
          taskShortFormArgument                               = self.edgeMethods.getEdgeAttribute(graph, fileNodeID, task, 'shortFormArgument')
          pipelineLongFormArgument, pipelineShortFormArgument = self.pipeline.getPipelineArgument(task, taskLongFormArgument)
          if not pipelineLongFormArgument: print('NOT HANDLED - configurationClass.checkRequiredFiles'); self.errors.terminate()
          else:
            description = self.pipeline.pipelineArguments[pipelineLongFormArgument].description
            self.errors.unsetFile(pipelineLongFormArgument, pipelineShortFormArgument, description)

  # Determine all of the graph dependencies.  This is essentially
  def determineGraphDependencies(self, graph, taskList, key):
    dependencies = []
    for task in taskList:
      for fileNodeID in self.nodeMethods.getPredecessorFileNodes(graph, task):
        #optionNodeID = self.nodeMethods.getOptionNodeIDFromFileNodeID(nodeID)

        # Determine if the file node has any predecessors.
        hasPredecessor = self.nodeMethods.hasPredecessor(graph, fileNodeID)

        # If there are no predecessors, find the name of the file and add to the list of dependencies.
        # Since the values associated with a node is a dictionary of lists, if 'key' is set to 'all',
        # get all of the values, otherwise, just get those with the specified key.
        if not hasPredecessor:
          values = self.nodeMethods.getGraphNodeAttribute(graph, fileNodeID, 'values')
          if key == 'all':
            for iteration in values.keys():
              for value in values[iteration]: dependencies.append((fileNodeID, value))

          # Just get values for a particular key.
          elif key in values:
            for value in values[key]: dependencies.append((fileNodeID, value))

          # TODO CHECK
          elif key not in values and key != 1:
             for value in values[1]: dependencies.append((fileNodeID, value))

          # If the key is unknown, fail.
          #TODO Errors.
          else:
            print('UNKNOWN KEY: configurationClass.determineGraphDependencies', key)
            print(values)
            self.errors.terminate()

    return dependencies

  # Determine all of the outputs.  This is essentially all file nodes with no predecessors.
  def determineGraphOutputs(self, graph, taskList, key):
    outputs     = []
    for task in taskList:
      for fileNodeID in self.nodeMethods.getSuccessorFileNodes(graph, task):
        optionNodeID = self.nodeMethods.getOptionNodeIDFromFileNodeID(fileNodeID)
        keepFiles    = self.nodeMethods.getGraphNodeAttribute(graph, optionNodeID, 'keepFiles')

        # Determine if the node has any successors.
        hasSuccessor = self.nodeMethods.hasSuccessor(graph, fileNodeID)

        # Get the tasks associated with this option node.
        tasks = graph.successors(optionNodeID)

        # If there are no successors, find the name of the file and add to the list of outputs.
        # Since the values associated with a node is a dictionary of lists, if 'key' is set to 'all',
        # get all of the values, otherwise, just get those with the specified key.
        if not hasSuccessor or keepFiles:
          values = self.nodeMethods.getGraphNodeAttribute(graph, fileNodeID, 'values')
          if key == 'all':
            for iteration in values.keys():
              for value in values[iteration]: outputs.append((optionNodeID, value))
  
          # Just get values for a particular key.
          elif key in values:
            for value in values[key]: outputs.append((optionNodeID, value))
  
          #TODO CHECK
          elif key not in values and key != 1:
            for value in values[1]: outputs.append((optionNodeID, value))

          # If the key is unknown, fail.
          #TODO Errors.
          else:
            print('UNKNOWN KEY: configurationClass.determineGraphOutputs', key)
            self.errors.terminate()

    return outputs

  # Determine all of the intermediate files in the graph.  This is all of the file nodes that have both
  # predecessor and successor nodes.
  def determineGraphIntermediateFiles(self, graph, taskList, key):
    intermediates = []
    seenNodes     = {}
    for task in taskList:
      for fileNodeID in self.nodeMethods.getPredecessorFileNodes(graph, task):
        optionNodeID = self.nodeMethods.getOptionNodeIDFromFileNodeID(fileNodeID)
        keepFiles    = self.nodeMethods.getGraphNodeAttribute(graph, optionNodeID, 'keepFiles')

        # Determine if the node has any predecessors or successors.
        hasPredecessor = self.nodeMethods.hasPredecessor(graph, fileNodeID)
        hasSuccessor   = self.nodeMethods.hasSuccessor(graph, fileNodeID)

        # Store this node in the list of nodes that have been checked. This ensures that the same nodes
        # values aren't added to the list multiple times. For example, if a file is produced by one task
        # and is then used by multiple subsequent tasks, the same node will come up for each of the tasks
        # that use the file, but it should only be listed as an intermediate file once.
        if fileNodeID not in seenNodes:
          seenNodes[fileNodeID] = True
          if hasPredecessor and hasSuccessor and not keepFiles:
            values = self.nodeMethods.getGraphNodeAttribute(graph, fileNodeID, 'values')

            # Do not include streaming nodes.
            if not self.edgeMethods.getEdgeAttribute(graph, fileNodeID, task, 'isStreaming'):
              if key == 'all':
                for iteration in values.keys():
                  for value in values[iteration]: intermediates.append((optionNodeID, value))
    
              # Just get values for a particular key.
              elif key in values:
                for value in values[key]: intermediates.append((optionNodeID, value))
    
              # TODO CHECK
              elif key != 1:
                for value in values[1]: intermediates.append((optionNodeID, value))
    
              # If the key is unknown, fail.
              #TODO Errors.
              else:
                print('UNKNOWN KEY: configurationClass.determineGraphIntermediateFiles', key)
                self.errors.terminate()

    return intermediates

  # Deterrmine when each intermediate file is last used,
  def setWhenToDeleteFiles(self, graph, intermediates):
    deleteList = {}
    for nodeID, filename in intermediates:

      # Find the successor task nodes.
      successorNodeIDs = graph.successors(nodeID)

      # Determine which of these tasks comes last in the workflow.
      for task in reversed(self.pipeline.workflow):
        if task in successorNodeIDs: break

      # Store the task when the file can be deleted.
      if filename in deleteList:
        # TODO ERROR
        print('SAME FILENAME', filename, 'appears multiple times in the list of intermediate files - setWhenToDeleteFiles')
        self.errors.terminate()

      if task not in deleteList: deleteList[task] = []
      deleteList[task].append(filename)

    return deleteList

  # Get all of the outputs from a task.
  def getTaskOutputs(self, graph, task, iteration):
    outputIDs = []
    for fileNodeID in self.nodeMethods.getSuccessorFileNodes(graph, task):

      # If this node is a streaming node, no output file is produced, so do not include
      # it in the lise.
      if not self.nodeMethods.getGraphNodeAttribute(graph, fileNodeID, 'isStreaming'):
        values = self.nodeMethods.getGraphNodeAttribute(graph, fileNodeID, 'values')
        if iteration == 'all':
          for counter in values:
            for value in values[counter]: outputIDs.append(value)
  
        elif iteration in values:
          for value in values[iteration]: outputIDs.append(value)
  
        elif iteration != 1:
          for value in values[1]: outputIDs.append(value)
  
        else:
          #TODO ERROR
          print('Unknown iteration in getTaskOutputs.')
          self.errors.terminate()

    return outputIDs

  # Get all of the dependencies for a task.
  def getTaskDependencies(self, graph, task, iteration):
    dependencies = []
    for fileNodeID in self.nodeMethods.getPredecessorFileNodes(graph, task):

      # If this node is a streaming node, no output file is produced, so do not include
      # it in the lise.
      if not self.nodeMethods.getGraphNodeAttribute(graph, fileNodeID, 'isStreaming'):
        values = self.nodeMethods.getGraphNodeAttribute(graph, fileNodeID, 'values')
        if iteration == 'all':
          for counter in values:
            for value in values[counter]: dependencies.append(value)
  
        elif iteration in values:
          for value in values[iteration]: dependencies.append(value)
  
        elif iteration != 1:
          for value in values[1]: dependencies.append(value)
  
        else:
          #TODO ERROR
          print('Unknown iteration in getTaskDependencies.')
          self.errors.terminate()

    return dependencies

  # For each task, determine the maximum number of datasets associated with any option.
  def getNumberOfDataSets(self, graph):
    for task in self.pipeline.workflow:
      totalNumber = 0
      isGreedy    = False
      for nodeID in self.nodeMethods.getPredecessorOptionNodes(graph, task):
        numberOfDataSets = len(self.nodeMethods.getGraphNodeAttribute(graph, nodeID, 'values'))
        if numberOfDataSets > totalNumber: totalNumber = numberOfDataSets

        # Check if this option is greedy. If the task has a greedy argument, then the number
        # of data sets is one.
        if self.edgeMethods.getEdgeAttribute(graph, nodeID, task, 'isGreedy'): isGreedy = True

      if isGreedy: self.nodeMethods.setGraphNodeAttribute(graph, task, 'numberOfDataSets', 1)
      else: self.nodeMethods.setGraphNodeAttribute(graph, task, 'numberOfDataSets', totalNumber)

  # Identify streaming file nodes.
  def identifyStreamingNodes(self, graph):
    for task in self.pipeline.workflow:

      # Parse the output nodes.
      if self.nodeMethods.getGraphNodeAttribute(graph, task, 'outputStream'):
        for fileNodeID in self.nodeMethods.getSuccessorFileNodes(graph, task):

          # Get the argument for this file node and check if this argument has the 'if output to stream'
          # option in the tool configuration file. Only one argument can have this option, so once it is
          # found, there is no need to check other file nodes for this task. If none of the arguments
          # have this option, terminate gkno, since the task is not able to output to a stream (the
          # configuration file would need to be updated to reflect this option).
          argument = self.edgeMethods.getEdgeAttribute(graph, task, fileNodeID, 'longFormArgument')
          tool     = self.nodeMethods.getGraphNodeAttribute(graph, task, 'tool')
          if self.tools.getArgumentAttribute(tool, argument, 'outputStream') != None:
            self.nodeMethods.setGraphNodeAttribute(graph, fileNodeID, 'isStreaming', True)

            # Mark the edges for this file node/option node -> task as streaming.
            optionNodeID = self.nodeMethods.getOptionNodeIDFromFileNodeID(fileNodeID)
            self.edgeMethods.setEdgeAttribute(graph, task, fileNodeID, 'isStreaming', True)
            self.edgeMethods.setEdgeAttribute(graph, optionNodeID, task, 'isStreaming', True)

            # Modify the command line argument associated with the nodes in order to handle the streaming
            # file.
            if self.edgeMethods.getEdgeAttribute(graph, optionNodeID, task, 'ifOutputIsStream') == 'do not include':
              self.edgeMethods.setEdgeAttribute(graph, optionNodeID, task, 'includeOnCommandLine', False)
              self.edgeMethods.setEdgeAttribute(graph, task, fileNodeID, 'includeOnCommandLine', False)

            else:
              #TODO ERROR
              print('config.identifyStreamingNodes.')
              self.errors.terminate()

            # The previous steps marked the edges for the output of the task as streaming. This necessarily
            # means that the input to the next task is a stream and so the corresponding edges that use
            # this input stream must be marked.
            for successorTask in self.nodeMethods.getSuccessorTaskNodes(graph, fileNodeID):
              successorOptionNodeID = self.nodeMethods.getOptionNodeIDFromFileNodeID(fileNodeID)
              self.edgeMethods.setEdgeAttribute(graph, fileNodeID, successorTask, 'isStreaming', True)
              self.edgeMethods.setEdgeAttribute(graph, successorOptionNodeID, successorTask, 'isStreaming', True)

            #TODO CHECK IF I NEED TO MODIFY COMMAND LINE ARGUMENTS HERE.
            break

  # Search for unset flag nodes and set the values to 'unset'.
  def searchForUnsetFlags(self, graph):
    for nodeID in self.nodeMethods.getNodes(graph, 'option'):
      if self.nodeMethods.getGraphNodeAttribute(graph, nodeID, 'dataType') == 'flag':
        if not self.nodeMethods.getGraphNodeAttribute(graph, nodeID, 'values'):
          self.nodeMethods.addValuesToGraphNode(graph, nodeID, ['unset'], write = 'replace')

  # Export an instance to file.
  def exportInstance(self, graph, path, runName, isPipeline):
    isVerbose = self.nodeMethods.getGraphNodeAttribute(graph, 'GKNO-VERBOSE', 'values')[1][0]

    # Check that only a single instance was specified and get the requested instance name.
    if len(self.nodeMethods.getGraphNodeAttribute(graph, 'GKNO-EXPORT-INSTANCE', 'values')[1]) > 1:
      self.errors.exportInstanceSetMultipleTimes(runName, isVerbose)

    # Set the filename and the instance name.
    filename     = runName + '_instances.json'
    instanceName = self.nodeMethods.getGraphNodeAttribute(graph, 'GKNO-EXPORT-INSTANCE', 'values')[1][0]

    # If no instance name was provided, or the instance already exists, fail.
    if instanceName == '': self.errors.noInstanceNameInExport(filename, instanceName, isVerbose)
    if instanceName in self.instances.instanceAttributes[runName]: self.errors.instanceNameExists(instanceName, isVerbose)

    # Get all of the arguments set by the user.
    if isPipeline: arguments = self.getAllPipelineArguments(graph)
    else: arguments = self.getAllToolArguments(graph, runName)
    if not arguments: self.errors.noInformationProvidedForInstance(isVerbose)
    self.instances.writeNewConfigurationFile(arguments, path, filename, runName, instanceName)

  # Get all of the argument data ready for sending to the instance file.
  def getAllPipelineArguments(self, graph):
    arguments = []
    for argument in self.pipeline.pipelineArguments:
      nodeID        = self.pipeline.pipelineArguments[argument].ID
      values        = self.nodeMethods.getGraphNodeAttribute(graph, nodeID, 'values')
      isConstructed = self.nodeMethods.getGraphNodeAttribute(graph, nodeID, 'isConstructed')

      # Only store arguments that were set by the user, not constructed.
      if not isConstructed and values: arguments.append((str(argument), values))

    return arguments 

  # Get all of the argument data for the tool.
  def getAllToolArguments(self, graph, runName):
    arguments = []
    for optionNodeID in self.nodeMethods.getPredecessorOptionNodes(graph, runName):
      argument      = self.edgeMethods.getEdgeAttribute(graph, optionNodeID, runName, 'longFormArgument')
      values        = self.nodeMethods.getGraphNodeAttribute(graph, optionNodeID, 'values')
      isConstructed = self.nodeMethods.getGraphNodeAttribute(graph, optionNodeID, 'isConstructed')
      if not isConstructed and values: arguments.append((str(argument), values))

    return arguments
