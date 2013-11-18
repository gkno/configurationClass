#!/usr/bin/python

from __future__ import print_function

import os
import sys

class configurationClassErrors:

  # Initialise.
  def __init__(self):
    self.hasError  = False
    self.errorType = 'ERROR'
    self.text      = []

  # Format the error message and write to screen.
  def writeFormattedText(self):
      firstLine = True
      secondLine = False
      maxLength = 93 - 5
      print(file = sys.stderr)
      for line in self.text:
        textList = []
        while len(line) > maxLength:
          index = line.rfind(' ', 0, maxLength)
          if index == -1:
            index = line.find(' ', 0, len(line))
            if index == -1:
              textList.append(line)
              line = ''
            else:
              textList.append(line[0:index])
              line = line[index + 1:len(line)]
          else:
            textList.append(line[0:index])
            line = line[index + 1:len(line)]

        if line != '' and line != ' ': textList.append(line)
        line = textList.pop(0)
        while line.startswith(' '): line = line[1:]

        if firstLine and self.errorType == 'ERROR':
          print('ERROR:   %-*s' % (1, line), file=sys.stderr)
        elif firstLine and self.errorType == 'WARNING':
          print('WARNING:   %-*s' % (1, line), file=sys.stderr)
        elif secondLine:
          print('DETAILS: %-*s' % (1, line), file=sys.stderr)
          secondLine = False
        else:
          print('         %-*s' % (1, line), file=sys.stderr)
        for line in textList:
          while line.startswith(' '): line = line[1:]

          if secondLine: print('DETAILS: %-*s' % (1, line), file=sys.stderr)
          else: print('         %-*s' % (1, line), file=sys.stderr)

        if firstLine:
          print(file=sys.stderr)
          firstLine = False
          secondLine = True

  #############################################
  # Errors with handling configuration files. #
  #############################################

  # If a configuration file cannot be found.
  def missingFile(self, filename):
    self.text.append('Missing configuration file.')
    self.text.append('The file \'' + filename + '\' could not be located."')
    self.writeFormattedText()
    self.terminate()

  # If there are errors with decoding the json file.
  def jsonError(self, error, filename):
    self.text.append('Malformed json file: ' + filename)
    self.text.append(str(error) + '.')
    self.writeFormattedText()
    self.terminate()

  #######################################
  # Errors associated with node values. #
  #######################################

  ###################################################
  # Errors associated with getting node attributes. *
  ###################################################

  # If a a node attribute was requested, but the node does not exist in the graph, terminate.
  def missingNodeInAttributeRequest(self, node):
    self.text.append('Unknown pipeline graph node attribute requested.')
    text = 'A pipeline node attribute was requested (using function getGraphNodeAttribute), however, the requested node \'' + node + \
    '\' does not exist in the pipeline graph.'
    self.text.append(text)
    self.writeFormattedText()
    self.terminate()

  # If a a node attribute was requested, but the node does not have an associated attributes, terminate.
  def noAttributesInAttributeRequest(self, node):
    text = 'Requested attributes from a pipeline graph node with no assigned attributes.'
    self.text.append(text)
    text = 'A pipeline node attribute was requested (using function getGraphNodeAttribute), however, no attributes have been attached to node \'' + \
    node + '\'.'
    self.text.append(text)
    self.writeFormattedText()
    self.terminate()

  # If a a node attribute was requested, but the node does not have the requested attribute, terminate.
  def attributeNotAssociatedWithNodeNoGraph(self, attribute, nodeType, inTaskNode, inFileNode, inOptionsNode):
    text = 'Requested a non-existent attribute from a non-graph node.'
    self.text.append(text)
    text = 'A pipeline node attribute was requested (using function getGraphNodeAttributeNoGraph), however, the requested attribute \'' + \
    attribute + '\' is not associated with the supplied node.'
    self.text.append(text)
    self.text.append('\t')

    # Check the node types to see if the requested attribute is available for some node types.
    if inTaskNode or inFileNode or inOptionsNode:
      text = 'The supplied node is a ' + nodeType + ' and the requested attribute is only available in the following node types:'
      self.text.append(text)
      if inTaskNode: self.text.append('\ttask nodes')
      if inFileNode: self.text.append('\tfile nodes')
      if inOptionsNode: self.text.append('\toptions nodes')
    else:
      text = 'The requested attribute is not associated with any of the available node data structures.'
      node + '\'.'
      self.text.append(text)
    self.writeFormattedText()
    self.terminate()

  # If a a node attribute was requested, but the node does not have the requested attribute, terminate.
  def attributeNotAssociatedWithNode(self, node, attribute, nodeType, inTaskNode, inFileNode, inOptionsNode):
    text = 'Requested a non-existent attribute from a pipeline graph node.'
    self.text.append(text)
    text = 'A pipeline node attribute was requested (using function getGraphNodeAttribute), however, the requested attribute \'' + attribute + \
    '\' is not associated with the supplied node \'' + node + '\'.  '
    self.text.append(text)
    self.text.append('\t')

    # Check the node types to see if the requested attribute is available for some node types.
    if inTaskNode or inFileNode or inOptionsNode:
      text = 'The supplied node is a ' + nodeType + ' and the requested attribute is only available in the following node types:'
      self.text.append(text)
      if inTaskNode: self.text.append('\ttask nodes')
      if inFileNode: self.text.append('\tfile nodes')
      if inOptionsNode: self.text.append('\toptions nodes')
    else:
      text = 'The requested attribute is not associated with any of the available node data structures.'
      node + '\'.'
      self.text.append(text)
    self.writeFormattedText()
    self.terminate()

  ################################################
  # Errors associated with setting node attributes
  ################################################

  # If a a node attribute was set, but the node does not exist in the graph, terminate.
  def missingNodeInAttributeSet(self, node):
    text = 'Attempt to set unknown pipeline graph node.'
    self.text.append(text)
    text = 'An attempt to set a pipeline node attribute was made (using function setGraphNodeAttribute), however, the requested node \'' + node + \
    '\' does not exist in the pipeline graph.'
    self.text.append(text)
    self.writeFormattedText()
    self.terminate()

  # If a a node attribute was set, but the node does not have an associated attributes, terminate.
  def noAttributesInAttributeSet(self, node):
    text = 'Attempt to set attributes from a pipeline graph node with no assigned attributes.'
    self.text.append(text)
    text = 'An attempt to set a pipeline node attribute was made (using function setGraphNodeAttribute), however, no attributes have been attached ' + \
    'to node \'' + node + '\'.'
    self.text.append(text)
    self.writeFormattedText()
    self.terminate()

  # If a a node attribute was set, but the node does not have the requested attribute, terminate.
  def attributeNotAssociatedWithNodeInSet(self, node, attribute, nodeType, inTaskNode, inFileNode, inOptionsNode):
    text = 'Attempt to set a non-existent attribute from a pipeline graph node.'
    self.text.append(text)
    text = 'An attempt to set a pipeline node attribute was made (using function setGraphNodeAttribute), however, the requested attribute \'' + \
    attribute + '\' is not associated with the supplied node \'' + node + '\'.  '
    self.text.append(text)
    self.text.append('\t')

    # Check the node types to see if the requested attribute is available for some node types.
    if inTaskNode or inFileNode or inOptionsNode:
      text = 'The supplied node is a ' + nodeType + ' and the requested attribute is only available in the following node types:'
      self.text.append(text)
      if inTaskNode: self.text.append('\ttask nodes')
      if inFileNode: self.text.append('\tfile nodes')
      if inOptionsNode: self.text.append('\toptions nodes')
    else:
      text = 'The requested attribute is not associated with any of the available node data structures.'
      self.text.append(text)
    self.writeFormattedText()
    self.terminate()

  # If a a node attribute was set, but the node does not have the requested attribute, terminate.  This is for setting
  # values in nodes not in the graph.
  def attributeNotAssociatedWithNodeInSetNoGraph(self, attribute, nodeType, inTaskNode, inFileNode, inOptionsNode):
    text = 'Attempt to set a non-existent attribute from a non-pipeline graph node.'
    self.text.append(text)
    text = 'An attempt to set a node attribute was made (using function setNodeAttribute), however, the requested attribute \'' + \
    attribute + '\' is not associated with the supplied node.'
    self.text.append(text)
    self.text.append('\t')

    # Check the node types to see if the requested attribute is available for some node types.
    if inTaskNode or inFileNode or inOptionsNode:
      text = 'The supplied node is a ' + nodeType + ' and the requested attribute is only available in the following node types:'
      self.text.append(text)
      if inTaskNode: self.text.append('\ttask nodes')
      if inFileNode: self.text.append('\tfile nodes')
      if inOptionsNode: self.text.append('\toptions nodes')
    else:
      text = 'The requested attribute is not associated with any of the available node data structures.'
      self.text.append(text)
    self.writeFormattedText()
    self.terminate()

  ################################################
  # Errors associated with getting edge attributes
  ################################################

  # Node is missing.
  def noNodeInGetEdgeAttribute(self, sourceNode, targetNode, missingNode):
    text = 'Attempt to get attribute for a non-existent edge.'
    self.text.append(text)
    text = 'An attempt to get information about an edge between nodes \'' + sourceNode + '\' and \'' + targetNode + '\' was made, however, node \''
    if missingNode == 'source': text += sourceNode
    elif missingNode == 'target': text += targetNode
    text += '\' does not exist.'
    self.text.append(text)
    self.writeFormattedText()
    self.terminate()

  # If the edge being interrogated has no attributes block.
  def noAttributesForEdge(self, sourceNode, targetNode):
    text = 'Attempt to get attribute for an edge with no attributes block.'
    self.text.append(text)
    text = 'An attempt to get information about an edge between nodes \'' + sourceNode + '\' and \'' + targetNode + '\' was made, however, this ' + \
    'edge does not have an attributes block defined.'
    self.text.append(text)
    self.writeFormattedText()
    self.terminate()

  # If the edge being interrogated has no attributes block.
  def invalidAttributeForEdge(self, sourceNode, targetNode, attribute):
    text = 'Attempt to get non-existent attribute for an edge.'
    self.text.append(text)
    text = 'An attempt to get information about an edge between nodes \'' + sourceNode + '\' and \'' + targetNode + '\' was made, however, the ' + \
    'requested attribute \'' + attribute + '\' is not associated with the edge.'
    self.text.append(text)
    self.writeFormattedText()
    self.terminate()

  #################################################
  # Errors associated with setting tool attributes.
  #################################################

  # If the tool attributes for an invalid tool are set.
  def invalidToolInSetToolAttribute(self, tool):
    text = 'Attempt to set tool attributes for an invalid tool: ' + tool
    self.text.append(text)
    text = 'A call was made to a function (setToolAttribute) to set tool attributes.  The tool \'' + tool + '\' does not exist, so no ' + \
    'data can be set for this tool.'
    self.text.append(text)
    self.writeFormattedText()
    self.terminate()

  # If the tool attributes for an tool include an invalid attribute.
  def invalidAttributeInSetToolAttribute(self, attribute):
    text = 'Attempt to set an invalid attribute for tool.'
    self.text.append(text)
    text = 'A call was made to a function (setToolAttribute) to set tool attributes.  The attribute \'' + attribute + '\' does not exist, so ' + \
    'this attribute cannot be set.'
    self.text.append(text)
    self.writeFormattedText()
    self.terminate()

  ####################################################
  # Errors associated with extracting tool information
  ####################################################

  # If data about an invalid tool is requested.
  def invalidTool(self, tool, function):
    text = 'Requested data about an invalid tool: ' + tool
    self.text.append(text)
    text = 'A call was made to a function (' + function + ') to extract tool information.  The tool \'' + tool + '\' does not exist, so no ' + \
    'data exists can be extracted.'
    self.text.append(text)
    self.writeFormattedText()
    self.terminate()
    
  # If no arguments data has been supplied for the tool.
  def noArgumentsInformation(self, tool, function):
    text = 'No arguments information for tool: ' + tool
    self.text.append(text)
    text = 'A call was made to a function (' + function + ') to extract argument information for tool \'' + tool + '\'.  No argument ' + \
    'is available for this tool.'
    self.text.append(text)
    self.writeFormattedText()
    self.terminate()
    
  # If the supplied argument is not available for the specified tool.
  def invalidToolArgument(self, tool, argument, function):
    text = 'Invalid argument.'
    self.text.append(text)
    text = 'A call was made to a function (' + function + ') to extract information about the argument \'' + argument + '\' associated ' + \
    'with tool \'' + tool + '\'.  This is an invalid argument for this tool.'
    self.text.append(text)
    self.writeFormattedText()
    self.terminate()

  ##############################
  # Terminate configurationClass
  ##############################

  def terminate(self):
    print(file=sys.stderr)
    print('================================================================================================', file=sys.stderr)
    print('  TERMINATED: Errors in configurationClass.  See specific error messages above for resolution.', file=sys.stderr)
    print('================================================================================================', file=sys.stderr)
    exit(2)
