#!/bin/bash/python

from __future__ import print_function
import networkx as nx
from copy import deepcopy

import configurationClassErrors
from configurationClassErrors import *

import edgeAttributes
from edgeAttributes import *

import json
import operator
import os
import sys

# Define a class for holding attributes for task nodes.
class taskNodeAttributes:
  def __init__(self):
    self.description      = 'No description provided'
    self.nodeType         = 'task'
    self.numberOfDataSets = 0
    self.tool             = ''

# Define a class for holding attributes for options nodes.  These are nodes that
# hold option data, but are not files.
class optionNodeAttributes:
  def __init__(self):
    self.allowedExtensions   = []
    self.allowMultipleValues = False
    self.associatedFileNodes = []
    self.dataType            = ''
    self.description         = 'No description provided'
    self.filenameExtensions  = ''
    self.hasMultipleDataSets = False
    self.hasMultipleValues   = False
    self.hasValue            = False
    self.isFile              = False
    self.isFilenameStub      = False
    self.isInput             = False
    self.isOutput            = False
    self.isPipelineArgument  = False
    self.isRequired          = False
    self.isStream            = False
    self.keepFiles           = False
    self.nodeType            = 'option'
    self.numberOfDataSets    = 0
    self.values              = {}

    # Node markings for node removal.
    self.isMarkedForRemoval = False

# Define a class for holding attributes for file nodes.  These are nodes that
# hold information about files.
class fileNodeAttributes:
  def __init__(self):
    self.allowMultipleValues = False
    self.allowedExtensions   = []
    self.description         = 'No description provided'
    self.hasMultipleDataSets = False
    self.hasMultipleValues   = False
    self.hasValue            = False
    self.nodeType            = 'file'
    self.numberOfDataSets    = 0
    self.values              = {}
 
    # Node markings for node removal.
    self.isMarkedForRemoval = False

class nodeClass:
  def __init__(self):
    self.edgeMethods  = edgeClass()
    self.errors       = configurationClassErrors()
    self.optionNodeID = 1

  # Build an option node.
  def buildOptionNode(self, graph, tools, task, tool, argument, attributes):
    nodeID = str('OPTION_') + str(self.optionNodeID)
    self.optionNodeID += 1
    graph.add_node(nodeID, attributes = attributes)

    # Add an edge to the task node.
    self.edgeMethods.addEdge(graph, self, tools, nodeID, task, argument)

    # If the node represents an option for defining an input or output file, create
    # a file node.
    shortForm = tools.getArgumentData(tool, argument, 'short form argument')
    if self.getGraphNodeAttribute(graph, nodeID, 'isInput'): self.buildTaskFileNodes(graph, tools, nodeID, task, argument, shortForm, 'input')
    elif self.getGraphNodeAttribute(graph, nodeID, 'isOutput'): self.buildTaskFileNodes(graph, tools, nodeID, task, argument, shortForm, 'output')

    return nodeID

  # Build a node using information from the tool configuration file.
  def buildNodeFromToolConfiguration(self, tools, tool, argument):
    isFilenameStub = False

    # Set the tool argument information.
    contents   = tools.configurationData[tool]['arguments'][argument]
    attributes = optionNodeAttributes()
    self.setNodeAttribute(attributes, 'dataType', contents['type'])
    self.setNodeAttribute(attributes, 'description', contents['description'])
    self.setNodeAttribute(attributes, 'isInput', contents['input'])
    self.setNodeAttribute(attributes, 'isOutput', contents['output'])
    if contents['input'] or contents['output']: self.setNodeAttribute(attributes, 'isFile', True)
    self.setNodeAttribute(attributes, 'isRequired', contents['required'])
    if 'allow multiple values' in contents: self.setNodeAttribute(attributes, 'allowMultipleValues', contents['allow multiple values'])
    if 'is filename stub' in contents:
      self.setNodeAttribute(attributes, 'isFilenameStub', contents['is filename stub'])

      # If the option node refers to a filename stub, a list of output files must also be present.
      try: extensions = contents['filename extensions']
      except:
        #TODO DEAL WITH ERROR.
        print('filename stub, but no filename extensions provided.', tool, argument)
        self.errors.terminate()

      for counter, extension in enumerate(extensions): extensions[counter] = str(extension)
      self.setNodeAttribute(attributes, 'filenameExtensions', extensions)
      isFilenameStub = True

    # If multiple extensions are allowed, they will be separated by pipes in the configuration
    # file.  Add all allowed extensions to the list.
    if not isFilenameStub:
      extension = contents['extension']
      if '|' in extension:
        extensions       = extension.split('|')
        stringExtensions = []
        for extension in extensions: stringExtensions.append(str(extension))
        self.setNodeAttribute(attributes, 'allowedExtensions', stringExtensions)
  
      #else: attributes.allowedExtensions.append(extension)
      else:
        extensions = []
        extensions.append(str(extension))
        self.setNodeAttribute(attributes, 'allowedExtensions', extensions)

    return attributes

  # Add input file nodes.
  def buildTaskFileNodes(self, graph, tools, nodeID, task, argument, shortForm, fileType):

    # Check if the node argument is a filename stub.  If it is, there are multiple file nodes to be
    # created.
    fileNodeIDs = []
    if self.getGraphNodeAttribute(graph, nodeID, 'isFilenameStub'):
      fileID = 1
      for extension in self.getGraphNodeAttribute(graph, nodeID, 'filenameExtensions'):
        attributes                     = fileNodeAttributes()
        attributes.description         = self.getGraphNodeAttribute(graph, nodeID, 'description')
        attributes.allowMultipleValues = self.getGraphNodeAttribute(graph, nodeID, 'allowMultipleValues')
        fileNodeID                     = nodeID + '_FILE_' + str(fileID)
        fileID += 1
        attributes.allowedExtensions = str(extension)
        graph.add_node(fileNodeID, attributes = attributes)
        fileNodeIDs.append(fileNodeID)
    else:
      fileNodeID = nodeID + '_FILE'
      attributes                     = fileNodeAttributes()
      attributes.description         = self.getGraphNodeAttribute(graph, nodeID, 'description')
      attributes.allowMultipleValues = self.getGraphNodeAttribute(graph, nodeID, 'allowMultipleValues')
      attributes.allowedExtensions   = self.getGraphNodeAttribute(graph, nodeID, 'allowedExtensions')
      graph.add_node(fileNodeID, attributes = attributes)
      fileNodeIDs.append(fileNodeID)

    # Add the edges.
    for fileNodeID in fileNodeIDs:
      tool = self.getGraphNodeAttribute(graph, task, 'tool')
      if fileType == 'input': self.edgeMethods.addEdge(graph, self, tools, fileNodeID, task, argument)
      else: self.edgeMethods.addEdge(graph, self, tools, task, fileNodeID, argument)

      # Add the file node to the list of file nodes associated with the option node.
      self.setGraphNodeAttribute(graph, nodeID, 'associatedFileNodes', fileNodeID)

  # TODO FINISH THIS
  # Build a task node.
  def buildTaskNode(self, graph, tools, task, tool):
    attributes             = taskNodeAttributes()
    attributes.tool        = tool
    attributes.description = tools.getConfigurationData(attributes.tool, 'description')
    graph.add_node(task, attributes = attributes)

  # Build all of the predecessor nodes for the task and attach them to the task node.
  def buildRequiredPredecessorNodes(self, graph, tools, task, tool):
    for argument in tools.configurationData[tool]['arguments']:
      attributes = self.buildNodeFromToolConfiguration(tools, tool, argument)
      isRequired = self.getNodeAttribute(attributes, 'isRequired')
      if isRequired: self.buildOptionNode(graph, tools, task, tool, argument, attributes)

  # Check each option node and determine if a value is required.  This can be determined in one of two 
  # ways.  If any of the edges beginning at the option node correspond to a tool argument that is 
  # listed as required by the tool, or if the node corresponds to a command line argument that is 
  # listed as required.  If the node is a required pipeline argument, it has already been tagged as 
  # required. 
  def setRequiredNodes(self, graph, tools): 
 
    # Loop over all data nodes. 
    for nodeID in graph.nodes(data = False): 
      nodeType = self.getGraphNodeAttribute(graph, nodeID, 'nodeType') 
      if nodeType == 'option': 
        for edge in graph.edges(nodeID): 
          task           = edge[1] 
          associatedTool = self.getGraphNodeAttribute(graph, task, 'tool') 
          toolArgument   = self.edgeMethods.getEdgeAttribute(graph, nodeID, task, 'argument') 
          isRequired     = tools.getArgumentData(associatedTool, toolArgument, 'required') 
          if isRequired: self.setGraphNodeAttribute(graph, nodeID, 'isRequired', True) 
          break

  # Check if a node exists based on a task and an argument.
  def doesNodeExist(self, graph, task, argument):
    exists = False
    for sourceNode, targetNode in graph.in_edges(task):
      edgeArgument = self.edgeMethods.getEdgeAttribute(graph, sourceNode, targetNode, 'argument')
      nodeType     = self.getGraphNodeAttribute(graph, sourceNode, 'nodeType')
      if nodeType == 'option' and edgeArgument == argument:
        exists = True
        return sourceNode

    return None

  # Determine which nodes each pipeline argument points to.
  def getPipelineArgumentNodes(self, graph, pipeline, tools):
    for argument in pipeline.argumentData:
      nodeID           = pipeline.argumentData[argument].nodeID
      task             = pipeline.nodeTaskInformation[nodeID][0][0]
      taskArgument     = pipeline.nodeTaskInformation[nodeID][0][1]
      tool             = pipeline.tasks[task]
      foundArgument    = False
      predecessorNodes = self.getPredecessorOptionNodes(graph, task)
      for predecessorNodeID in predecessorNodes:
        testArgument = self.edgeMethods.getEdgeAttribute(graph, predecessorNodeID, task, 'argument')
        if taskArgument == testArgument:
          foundArgument = True
          break

      # If the node to which this argument points has not been defined, no edge linking the node
      # to the task will be found.  Thus, the node and edge need to be created.
      if not foundArgument:
        attributes = self.buildNodeFromToolConfiguration(tools, tool, taskArgument)
        predecessorNodeID = self.buildOptionNode(graph, tools, task, tool, taskArgument, attributes)

      # Set the nodeID to a graph nodeID.
      pipeline.argumentData[argument].nodeID = predecessorNodeID

  # Get an attribute from the nodes data structure.  Check to ensure that the requested attribute is
  # available for the type of node.  If not, terminate with an error.
  def getGraphNodeAttribute(self, graph, node, attribute):
    try: value = getattr(graph.node[node]['attributes'], attribute)

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
  def setGraphNodeAttribute(self, graph, nodeID, attribute, value):
    try:
      test = getattr(graph.node[nodeID]['attributes'], attribute)

    # If there is an error, determine if the node exists in the graph.  If the node exists, the problem
    # lies with the attribute.  Determine if the attribute belongs to any of the node data structures,
    # then terminate.
    except:
      if nodeID not in graph.nodes():
        self.errors.missingNodeInAttributeSet(nodeID)

      # If no attributes have been attached to the node.
      elif 'attributes' not in graph.node[nodeID]:
        self.errors.noAttributesInAttributeSet(nodeID)

      # If the attribute is not associated with the node.
      else:
        inTaskNode    = False
        inFileNode    = False
        inOptionsNode = False
        if hasattr(taskNodeAttributes(), attribute): inTaskNode      = True
        if hasattr(fileNodeAttributes(), attribute): inFileNode      = True
        if hasattr(optionNodeAttributes(), attribute): inOptionsNode = True
        if graph.node[nodeID]['attributes'].nodeType == 'task': nodeType = 'task node'
        if graph.node[nodeID]['attributes'].nodeType == 'file': nodeType = 'file node'
        if graph.node[nodeID]['attributes'].nodeType == 'option': nodeType = 'options node'
        self.errors.attributeNotAssociatedWithNodeInSet(nodeID, attribute, nodeType, inTaskNode, inFileNode, inOptionsNode)

    # Set the attribute.  If the attribute points to a list, append the value.
    if type(getattr(graph.node[nodeID]['attributes'], attribute)) == list:
      valueList = getattr(graph.node[nodeID]['attributes'], attribute)
      valueList.append(value)
      setattr(graph.node[nodeID]['attributes'], attribute, valueList)
    else:
      setattr(graph.node[nodeID]['attributes'], attribute, value)

  # Get an attribute from the nodes data structure.  Check to ensure that the requested attribute is
  # available for the type of node.  If not, terminate with an error.  This method is for a node not
  # contained in the graph.
  def getNodeAttribute(self, nodeAttributes, attribute):
    try: value = getattr(nodeAttributes, attribute)

    # If there is an error, determine if the node exists in the graph.  If the node exists, the problem
    # lies with the attribute.  Determine if the attribute belongs to any of the node data structures,
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
      self.errors.attributeNotAssociatedWithNodeNoGraph(attribute, nodeType, inTaskNode, inFileNode, inOptionsNode)

    return value

  # Set an attribute from the nodes data structure.  In this method, the node is not a part of the graph and
  # so the node itself is given to the method.
  def setNodeAttribute(self, nodeAttributes, attribute, value):
    try: test = getattr(nodeAttributes, attribute)

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

  # Add values to a node.
  def addValuesToGraphNode(self, graph, nodeID, values, write, iteration = None):

    # Since values have been added to the node, set the hasValue flag to True.
    self.setGraphNodeAttribute(graph, nodeID, 'hasValue', True)

    # If write is set to replace, set the number of datasets to 1, clear any values currently
    # set and add the new values.
    if write == 'replace':
      graph.node[nodeID]['attributes'].values    = {}
      graph.node[nodeID]['attributes'].values[1] = values
      self.setGraphNodeAttribute(graph, nodeID, 'numberOfDataSets', 1)

    # If write is set to append, append the value to the defined iteration.
    elif write == 'append':
      definedValues    = self.getGraphNodeAttribute(graph, nodeID, 'values')
      numberOfDataSets = self.getGraphNodeAttribute(graph, nodeID, 'numberOfDataSets')
                                                                   
      # If there are currently no data sets associated with this node, create the '1' iteration.
      if numberOfDataSets == 0 and iteration == 1:
        numberOfDataSets = 1
        definedValues[1] = []

      # Check that the defined iteration exists.
      try: iterationValues = definedValues[iteration]
      except:
        #TODO ERROR
        print('Unavailable iteration in addValuesToGraphNode')
        self.errors.terminate()

      for value in values: iterationValues.append(value)
      graph.node[nodeID]['attributes'].values[iteration] = iterationValues

    # If write is set to append, find the number of datasets, then append a new set of values.
    elif write == 'iteration':
      numberOfDataSets = self.getGraphNodeAttribute(graph, nodeID, 'numberOfDataSets')
      graph.node[nodeID]['attributes'].values[numberOfDataSets + 1] = values
      self.setGraphNodeAttribute(graph, nodeID, 'numberOfDataSets', numberOfDataSets + 1)

    # The write mode is either 'replace' or 'append'.  The 'replace' mode will remove any values
    # already attached to the node and replace them with those provided.  If the were multiple
    # iterations of values attached, this mode will remove them and leave only a single iteration 
    # of data (see multiple runs or internal loops).  The 'append' mode will generate a new iteration.
    else:
      #TODO ERROR
      print('Unknown write mode in addValuesToGraphNode -', write)
      self.errors.terminate()

  # Replace a nodes values.
  def replaceGraphNodeValues(self, graph, nodeID, values):

    # If replacing the values, the supplied values must be a dictionary.  If not, fail.
    # TODO Sort errors.
    if type(values) != dict:
      print('nodeMethods.replaceGraphNodeValues: Values not dict')
      print(values)
      self.errors.terminate()

    # Since values have been added to the node, set the hasValue flag to True.
    self.setGraphNodeAttribute(graph, nodeID, 'hasValue', True)

    # Determine how many sets of values are already included.
    numberOfDataSets = len(values)

    # Add the values.
    self.setGraphNodeAttribute(graph, nodeID, 'values', values)
    self.setGraphNodeAttribute(graph, nodeID, 'numberOfDataSets', numberOfDataSets)

  # Find all of the task nodes in the graph.
  def getNodes(self, graph, nodeType):
    nodeList = []
    for node in graph.nodes(data = False):
      if self.getGraphNodeAttribute(graph, node, 'nodeType') == nodeType: nodeList.append(node)

    return nodeList

  # Get the node associated with a tool argument.
  def getNodeForTaskArgument(self, graph, task, argument):
    nodeIDs          = []
    predecessorEdges = graph.in_edges(task)
    for predecessorEdge in predecessorEdges:
      value = self.edgeMethods.getEdgeAttribute(graph, predecessorEdge[0], predecessorEdge[1], 'argument')
      if value == argument:

        # Only return a value if this is an option node.
        if self.getGraphNodeAttribute(graph, predecessorEdge[0], 'nodeType') == 'option': nodeIDs.append(predecessorEdge[0])

    return nodeIDs

  # Get all file nodes associated with a task.
  def getFileNodeIDs(self, graph, task):
    nodeIDs = []

    # Get all of the predecessor file nodes.
    try: predecessorIDs = graph.predecessors(task)
    except:
      #TODO ERROR
      print('FAILED')
      self.errors.terminate()

    for nodeID in predecessorIDs:
      if self.getGraphNodeAttribute(graph, nodeID, 'nodeType') == 'file': nodeIDs.append(nodeID)

    # Get all of the successor file nodes.
    try: successorIDs = graph.successors(task)
    except:
      #TODO ERROR
      print('FAILED')
      self.errors.terminate()

    for nodeID in successorIDs:
      if self.getGraphNodeAttribute(graph, nodeID, 'nodeType') == 'file': nodeIDs.append(nodeID)

  # Get all predecessor file nodes for a task.
  def getPredecessorOptionNodes(self, graph, task):
    optionNodes = []

    try: predecessors = graph.predecessors(task)
    except:

      #TODO SORT OUT ERROR MESSAGE.
      print('failed')

    for predecessor in predecessors:
      if self.getGraphNodeAttribute(graph, predecessor, 'nodeType') == 'option': optionNodes.append(predecessor)

    return optionNodes

  # Get all predecessor file nodes for a task.
  def getPredecessorFileNodes(self, graph, task):
    fileNodes = []

    try: predecessors = graph.predecessors(task)
    except:

      #TODO SORT OUT ERROR MESSAGE.
      print('failed')

    for predecessor in predecessors:
      if self.getGraphNodeAttribute(graph, predecessor, 'nodeType') == 'file': fileNodes.append(predecessor)

    return fileNodes

  # Get all successor file nodes for a task.
  def getSuccessorFileNodes(self, graph, task):
    fileNodes = []

    try: successors = graph.successors(task)
    except:

      #TODO SORT OUT ERROR MESSAGE.
      print('failed')

    for successor in successors:
      if self.getGraphNodeAttribute(graph, successor, 'nodeType') == 'file': fileNodes.append(successor)

    return fileNodes

  # Given a file node ID, return the corresponding option node ID.
  def getOptionNodeIDFromFileNodeID(self, nodeID):
    return nodeID.split('_FILE')[0]

  # Determine if the supplied node has any predecessors.
  def hasPredecessor(self, graph, nodeID):
    predecessorNodeIDs = graph.predecessors(nodeID)
    if predecessorNodeIDs: return True
    else: return False

  # Determine if the supplied node has any successors.
  def hasSuccessor(self, graph, nodeID):
    successorNodeIDs = graph.successors(nodeID)
    if successorNodeIDs: return True
    else: return False

  # Get all of the file nodes associated with an option node.
  def getAssociatedFileNodeIDs(self, graph, optionNodeID):

    # Check that an option node was supplied.
    if self.getGraphNodeAttribute(graph, optionNodeID, 'nodeType') != 'option':
      #TODO SORT ERROR
      print('ATTEMPTING TO FIND FILE NODES ASSOCIATED WTH OPTION NODE, BUT AN OPTION NODE WAS NOT PROVIDED.')
      print(optionNodeID, 'nodeMethods.getAssociatedFileNodes')
      self.errors.terminate()

    fileNodeIDs = []
    for nodeID in graph.nodes(data = False):
      if self.getGraphNodeAttribute(graph, nodeID, 'nodeType') == 'file' and nodeID.startswith(optionNodeID + '_'): fileNodeIDs.append(nodeID)

    return fileNodeIDs

  # From a list of node IDs, find the node with a predecessor node. If more than one such node
  # is present in the list, terminate. If there are none, return a random node ID from the list.
  def getNodeIDWithPredecessor(self, graph, nodeIDs, task):
    foundNodeIDWithPredecessor = False
    for nodeID in nodeIDs:
      fileNodeIDs = self.getAssociatedFileNodeIDs(graph, nodeID)
      for fileNodeID in fileNodeIDs:
        predecessorNodeIDs = graph.predecessors(fileNodeID)
        if predecessorNodeIDs:
          if foundNodeIDWithPredecessor:
            # TODO SORT ERROR
            print('Multiple nodeIDs with predecessor - config.nodeMethods.getNodeIDWithPredecessor')
            self.errors.terminate()
          else:
            returnNodeID               = nodeID
            foundNodeIDWithPredecessor = True

    # If no nodes with a predecessor were found, return a random node ID.
    if not foundNodeIDWithPredecessor: return nodeIDs[0]
    else: return returnNodeID

  # Parse through all nodes and remove those that are marked for deletion.
  def purgeNodeMarkedForRemoval(self, graph, typeToRemove = 'all'):
    nodeIDs = []

    if typeToRemove != 'option' and typeToRemove != 'file' and typeToRemove != 'general' and typeToRemove != 'all':
      print('Unknown node type to remove - nodeMethods.purgeNodeMarkedForRemoval.')
      self.errors.terminate()

    # Identify option nodes if required.
    if typeToRemove == 'option' or typeToRemove == 'all': nodeIDs += self.getNodes(graph, 'option')

    # Identify file nodes if required.
    if typeToRemove == 'file' or typeToRemove == 'all': nodeIDs += self.getNodes(graph, 'file')

    # Identify general nodes if required.
    if typeToRemove == 'general' or typeToRemove == 'all': nodeIDs += self.getNodes(graph, 'general')

    for nodeID in nodeIDs:
      if self.getGraphNodeAttribute(graph, nodeID, 'isMarkedForRemoval'): graph.remove_node(nodeID)

  # Rename a node.  This involves creating a new node with the same attributes as the node being
  # removed.  Then reproduce all of the edges, before removing the old node.
  def renameNode(self, graph, tools, originalNodeID, newNodeID):
    graph.add_node(newNodeID, attributes = graph.node[originalNodeID]['attributes'])

    # Set all of the predecessor edges.
    predecessorNodeIDs = graph.predecessors(originalNodeID)
    for nodeID in predecessorNodeIDs:
      self.edgeMethods.addEdge(graph, self, tools, nodeID, newNodeID, self.edgeMethods.getEdgeAttribute(graph, nodeID, originalNodeID, 'argument'))

    # Set all of the successor edges.
    successorNodeIDs = graph.successors(originalNodeID)
    for nodeID in successorNodeIDs:
      self.edgeMethods.addEdge(graph, self, tools, newNodeID, nodeID, self.edgeMethods.getEdgeAttribute(graph, originalNodeID, nodeID, 'argument'))

    # Remove the original node.
    graph.remove_node(originalNodeID)
