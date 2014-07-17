#!/bin/bash/python

from __future__ import print_function
import networkx as nx
from copy import deepcopy

import configurationClassErrors
from configurationClassErrors import *

import fileOperations
from fileOperations import *

from collections import OrderedDict

import json
import os
import shutil
import sys

# Define a class to hold parameter set attributes.
class parameterSetAttributes:
  def __init__(self):

    # Define the parameter set name and description.
    self.description = None
    self.ID          = None

    # Define a dictionary to hold the information about the nodes (e.g. the argument and values).
    self.nodes = []

    # Record if the parameter set is held in the external parameter sets file.
    self.isExternal = False

  # Define a class to hold the parameter set information.
  class parameterSetNodeAttributes:
    def __init__(self):
      self.argument = None
      self.ID       = None
      self.values   = None

class parameterSetConfiguration:
  def __init__(self):

    # Define structures to hold parameter set information.
    self.parameterSetAttributes = {}

    # Define the errors class.
    self.errors = configurationClassErrors()

    # Define operations for operating on files.
    self.fileOperations = fileOperations()

  # Check the parameter set data.
  def checkParameterSets(self, runName, parameterSets, isPipeline, isExternal):

    # Define the allowed attributes.
    allowedAttributes                = {}
    allowedAttributes['description'] = (str, True, True, 'description')
    allowedAttributes['ID']          = (str, True, True, 'ID')
    allowedAttributes['nodes']       = (list, True, False, None)

    # Define allowed node attributes.
    allowedNodeAttributes = {}
    allowedNodeAttributes['argument'] = (str, True, True, 'argument')
    allowedNodeAttributes['ID']       = (str, True, True, 'ID')
    allowedNodeAttributes['values']   = (list, True, True, 'values')

    # Loop over all available parameter sets.
    for parameterSet in parameterSets:

      # Define the attributes object.
      attributes = parameterSetAttributes()

      # Keep track of the observed required values, argument and IDs.
      observedAttributes = {}
      observedIDs        = {}
      observedArguments  = {}

      # Get the name (ID) of this parameter set. This will be used for error messages and will be used as the
      # key for storing information about this parameter set. If this isn't present, terminate.
      try: parameterSetID = parameterSet['ID']
      except: self.errors.noIDForParameterSet(runName, isPipeline)

      # Now loop over the remaining attributes.
      for attribute in parameterSet:
        if attribute not in allowedAttributes: self.errors.invalidAttributeInParameterSet(runName, parameterSetID, attribute, allowedAttributes)

        # Mark the attribute as seen.
        observedAttributes[attribute] = True

        # Store the given attribtue.
        if allowedAttributes[attribute][2]: self.setAttribute(attributes, allowedAttributes[attribute][3], parameterSet[attribute])

      # Having parsed all of the general attributes, check that all those that are required
      # are present.
      for attribute in allowedAttributes:
        if allowedAttributes[attribute][1] and attribute not in observedAttributes:
          self.errors.missingAttributeInParameterSet(runName, attribute, allowedAttributes, parameterSetID, isPipeline)

      # Each parameter set has a 'nodes' section (and if the validation has reached this point, the
      # section is present and a dictionary as required). Check the contents of this section.
      for node in parameterSet['nodes']:

        # Define the attributes object.
        nodeAttributes = attributes.parameterSetNodeAttributes()

        # Keep track of the observed required values.
        observedAttributes = {}

        for attribute in node:
          if attribute not in allowedNodeAttributes:
            self.errors.invalidAttributeInParameterSetNode(runName, parameterSetID, attribute, allowedNodeAttributes, isPipeline)

          # Mark the attribute as seen.
          observedAttributes[attribute] = True

          # Store the given attribtue.
          if allowedNodeAttributes[attribute][2]: self.setAttribute(nodeAttributes, allowedNodeAttributes[attribute][3], node[attribute])

        # Having parsed all of the node attributes, check that all those that are required
        # are present.
        for attribute in allowedNodeAttributes:
          if allowedNodeAttributes[attribute][1] and attribute not in observedAttributes:
            self.errors.missingAttributeInPipelineParameterSetNode(runName, parameterSetID, attribute, allowedNodeAttributes, isPipeline)

        # Store the ID and argument associated with each node to check for duplicates. If they have already
        # been seen, terminate.
        if node['ID'] in observedIDs: self.errors.duplicateNodeInParameterSet(runName, parameterSetID, 'ID', node['ID'], isPipeline)
        if node['argument'] in observedArguments: self.errors.duplicateNodeInParameterSet(runName, parameterSetID, 'argument', node['argument'], isPipeline)
        observedIDs[node['ID']]             = True
        observedArguments[node['argument']] = True

        # Store the node data using the argument as the key.
        attributes.nodes.append(nodeAttributes)

      # If this parameter set is in the external parameter set file, maek it as such.
      attributes.isExternal = isExternal

      # Store the parameter set information.
      if runName not in self.parameterSetAttributes: self.parameterSetAttributes[runName] = {}
      if parameterSetID in self.parameterSetAttributes[runName]: self.errors.duplicateParameterSet(runName, parameterSetID, isPipeline)
      self.parameterSetAttributes[runName][parameterSetID] = attributes

    # Convert any unicode values to strings.
    self.convertUnicode()

  # Convert any unicode values to strings.
  def convertUnicode(self):
    for runName in self.parameterSetAttributes:
      for parameterSet in self.parameterSetAttributes[runName]:
        for nodeCount, node in enumerate(self.parameterSetAttributes[runName][parameterSet].nodes):
          for counter, value in enumerate(node.values):
            if isinstance(value, unicode): self.parameterSetAttributes[runName][parameterSet].nodes[nodeCount].values[counter] = str(value)

  # Check for parameterSets in external parameterSets file.
  def checkExternalParameterSets(self, fileOperations, filename, runName, tools, isPipeline):
    filename = filename.replace('.json', '_parameterSets.json')

    # Check if the file exists (it's existence is not necessary).
    if os.path.exists(filename):
      configurationData = fileOperations.readConfigurationFile(filename)
      self.checkParameterSets(runName, configurationData['parameterSets'], isPipeline, isExternal = True)

  # Set a value in the toolAttributes.
  def setAttribute(self, attributes, attribute, value):
    try: test = getattr(attributes, attribute)

    # If the attribute can't be set, determine the source of the problem and provide an
    # error message.
    except:

      # If the tool is not available.TODO
      self.errors.invalidAttributeInSetAttribute(attribute, True)
      self.errors.terminate()

    # Set the attribute.
    setattr(attributes, attribute, value)

    return attributes

  # Get a parameter set node attribute.
  def getArguments(self, runName, parameterSetName, isPipeline):
    arguments = []

    # Check that the parameter set exists.
    if parameterSetName not in self.parameterSetAttributes[runName]:
      self.errors.missingParameterSet(runName, parameterSetName, isPipeline, self.parameterSetAttributes[runName].keys())

    for node in self.parameterSetAttributes[runName][parameterSetName].nodes:
      arguments.append((node.argument, node.values))

    return arguments

  # Get the parameter set information or fail if the parameter set does not exist.
  def checkRequestedParameterSet(self, path, name, parameterSetName, parameterSetFiles, isPipeline):

    # Define the available parameterSets depending on whether this is a tool or pipeline.
    availableParameterSets = parameterSetFiles['pipeline parameter sets'] if isPipeline else parameterSetFiles['tool parameter sets']
    path                   = path + 'pipes/' if isPipeline else path + 'tools/'
    # If the defined parameter set is in the configuration file, nothing needs to be done. 

    if parameterSetName in self.parameterSetAttributes[name]: return
    # All parameterSets from the extenal file have now been added to the data structure. If the parameter set still does
    # not exist, the the parameter set isn't defined.
    if parameterSetName not in self.parameterSetAttributes[name]:
      self.errors.missingParameterSet(name, parameterSetName, isPipeline, self.parameterSetAttributes[name].keys())

  # Write out the new configuration file and move to the config_files/pipes directory.
  def writeNewConfigurationFile(self, arguments, path, filename, runName, parameterSetName):

    # Open the new file.
    filehandle = open(filename, 'w')

    # Add the new parameter set information to the parameterSetAttributes.
    attributes             = parameterSetAttributes()
    attributes.description = 'User specified parameter set'
    attributes.ID          = parameterSetName
    attributes.isExternal  = True
    self.parameterSetAttributes[runName][parameterSetName] = attributes

    counter = 1
    # Add the arguments and values to the nodes.
    for argument, values in arguments:

      # Put all of the values in a list.
      nodeAttributes           = attributes.parameterSetNodeAttributes()
      nodeAttributes.argument  = str(argument)
      nodeAttributes.ID        = str('node' + str(counter))
      nodeAttributes.values    = values[1]
      attributes.nodes.append(nodeAttributes)
      counter += 1

    # Put all of the parameter set information in a dictionary that can be dumped to a json file.
    jsonParameterSets              = OrderedDict()
    jsonParameterSets['parameterSets'] = []
    for parameterSet in self.parameterSetAttributes[runName]:

      # Only include parameterSets that were marked as external.
      if self.parameterSetAttributes[runName][parameterSet].isExternal:
        parameterSetInformation                = OrderedDict()
        parameterSetInformation['ID']          = parameterSet
        parameterSetInformation['description'] = self.parameterSetAttributes[runName][parameterSet].description
        parameterSetInformation['nodes']       = []

        # Set the nodes.
        for node in self.parameterSetAttributes[runName][parameterSet].nodes:
          nodeInformation              = OrderedDict()
          nodeInformation['ID']        = node.ID
          nodeInformation['argument']  = node.argument
          nodeInformation['values']    = node.values
          parameterSetInformation['nodes'].append(nodeInformation)

        # Store this parameterSets data.
        jsonParameterSets['parameterSets'].append(parameterSetInformation)

    # Dump all the parameterSets to file.
    json.dump(jsonParameterSets, filehandle, indent = 2)
    filehandle.close()

    # Move the configuration file.
    shutil.copy(filename, path)
    os.remove(filename)

    print(file = sys.stdout)
    print('=' * 66, file = sys.stdout)
    print('Configuration file generation complete.', file = sys.stdout)
    print('', file = sys.stdout)
    print('It is recommended that the new configuration is visually inspected', file = sys.stdout)
    print('and tested to ensure expected behaviour.', file = sys.stdout)
    print('=' * 66, file = sys.stdout)
    sys.stdout.flush()
