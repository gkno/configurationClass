#!/bin/bash/python

from __future__ import print_function
import networkx as nx
from copy import deepcopy

import configurationClassErrors
from configurationClassErrors import *

import fileOperations
from fileOperations import *

import json
import os
import sys

# Define an instance to hold instance attributes.
class instanceAttributes:
  def __init__(self):

    # Define the instance name and description.
    self.description = None
    self.ID          = None

    # Define a dictionary to hold the information about the nodes (e.g. the argument and values).
    self.nodes = []

  # Define a class to hold the instance node information.
  class instanceNodeAttributes:
    def __init__(self):
      self.argument = None
      self.ID       = None
      self.values   = None

class instanceConfiguration:
  def __init__(self):

    # Define structures to hold instance information.
    self.instanceAttributes = {}

    # Define the errors class.
    self.errors = configurationClassErrors()

    # Define operations for operating on files.
    self.fileOperations = fileOperations()

  # Check the instance data.
  def checkInstances(self, runName, instances, isPipeline):

    # Define the allowed attributes.
    allowedAttributes                = {}
    allowedAttributes['description'] = (str, True, True, 'description')
    allowedAttributes['ID']          = (str, True, True, 'ID')
    allowedAttributes['nodes']       = (dict, True, False, None)

    # Define allowed node attributes.
    allowedNodeAttributes = {}
    allowedNodeAttributes['argument'] = (str, True, True, 'argument')
    allowedNodeAttributes['ID']       = (str, True, True, 'ID')
    allowedNodeAttributes['values']   = (list, True, True, 'values')

    # Loop over all available instances.
    for instance in instances:

      # Define the attributes object.
      attributes = instanceAttributes()

      # Keep track of the observed required values, argument and IDs.
      observedAttributes = {}
      observedIDs        = {}
      observedArguments  = {}

      # Get the name (ID) of this instance. This will be used for error messages and will be used as the
      # key for storing information about this instance. If this isn't present, terminate.
      try: instanceID = instance['ID']
      except: self.errors.noIDForInstance(runName, isPipeline)

      # Now loop over the remaining attributes.
      for attribute in instance:
        if attribute not in allowedAttributes: self.errors.invalidAttributeInInstance(runName, instanceID, attribute, allowedAttributes, isPipeline)

        # Mark the attribute as seen.
        observedAttributes[attribute] = True

        # Store the given attribtue.
        if allowedAttributes[attribute][2]: self.setAttribute(attributes, allowedAttributes[attribute][3], instance[attribute])

      # Having parsed all of the general attributes, check that all those that are required
      # are present.
      for attribute in allowedAttributes:
        if allowedAttributes[attribute][1] and attribute not in observedAttributes:
          self.errors.missingAttributeInInstance(runName, attribute, allowedAttributes, instanceID, isPipeline)

      # Each instance has a 'nodes' section (and if the validation has reached this point, the
      # section is present and a dictionary as required). Check the contents of this section.
      for node in instance['nodes']:

        # Define the attributes object.
        nodeAttributes = attributes.instanceNodeAttributes()

        # Keep track of the observed required values.
        observedAttributes = {}

        for attribute in node:
          if attribute not in allowedNodeAttributes:
            self.errors.invalidAttributeInInstanceNode(runName, instanceID, attribute, allowedNodeAttributes, isPipeline)

          # Mark the attribute as seen.
          observedAttributes[attribute] = True

          # Store the given attribtue.
          if allowedNodeAttributes[attribute][2]: self.setAttribute(nodeAttributes, allowedNodeAttributes[attribute][3], node[attribute])

        # Having parsed all of the node attributes, check that all those that are required
        # are present.
        for attribute in allowedNodeAttributes:
          if allowedNodeAttributes[attribute][1] and attribute not in observedAttributes:
            self.errors.missingAttributeInPipelineInstanceNode(runName, instanceID, attribute, allowedNodeAttributes, isPipeline)

        # Store the ID and argument associated with each node to check for duplicates. If they have already
        # been seen, terminate.
        if node['ID'] in observedIDs: self.errors.duplicateNodeInInstance(runName, instanceID, 'ID', node['ID'], isPipeline)
        if node['argument'] in observedArguments: self.errors.duplicateNodeInInstance(runName, instanceID, 'argument', node['argument'], isPipeline)
        observedIDs[node['ID']]             = True
        observedArguments[node['argument']] = True

        # Store the node data using the argument as the key.
        attributes.nodes.append(nodeAttributes)

      # Store the instance information.
      if runName not in self.instanceAttributes: self.instanceAttributes[runName] = {}
      if instanceID in self.instanceAttributes[runName]: self.errors.duplicateInstance(runName, instanceID, isPipeline)
      self.instanceAttributes[runName][instanceID] = attributes

    # Convert any unicode values to strings.
    self.convertUnicode()

  # Convert any unicode values to strings.
  def convertUnicode(self):
    for runName in self.instanceAttributes:
      for instance in self.instanceAttributes[runName]:
        for nodeCount, node in enumerate(self.instanceAttributes[runName][instance].nodes):
          for counter, value in enumerate(node.values):
            if isinstance(value, unicode): self.instanceAttributes[runName][instance].nodes[nodeCount].values[counter] = str(value)

  # Check for instances in external instances file.
  def checkExternalInstances(self, fileOperations, filename, runName, tools, isPipeline):
    filename          = filename.replace('.json', '_instances.json')

    # Check if the file exists (it's existence is not necessary).
    if os.path.exists(filename):
      configurationData = fileOperations.readConfigurationFile(filename)
      self.checkInstances(runName, configurationData['instances'], isPipeline)

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

  # Get an instance node attribute.
  def getArguments(self, runName, instanceName):
    arguments = []
    for node in self.instanceAttributes[runName][instanceName].nodes:
      arguments.append((node.argument, node.values))

    return arguments

  # Get the instance information or fail if the instance does not exist.
  def checkRequestedInstance(self, path, name, instanceName, instanceFiles, isPipeline):

    # Define the available instances depending on whether this is a tool or pipeline.
    availableInstances = instanceFiles['pipeline instances'] if isPipeline else instanceFiles['tool instances']
    path = path + 'pipes/' if isPipeline else path + 'tools/'

    # If the defined instance is in the configuration file, nothing needs to be done. 
    if instanceName in self.instanceAttributes[name]: return

    # Check the external instance file (if one exists).
    if name + '_instances.json' in availableInstances.keys():
      filePath = path + name + '_instances.json'
      data     = self.fileOperations.readConfigurationFile(filePath)
      self.checkInstances(name, data['instances'], isPipeline)

      # All instances from the extenal file have now been added to the data structure. If the instance still does
      # not exist, the the instance isn't defined.
      if instanceName not in self.instanceAttributes[name]:
        self.errors.missingInstance(name, instanceName, isPipeline, self.instanceAttributes[name].keys())
