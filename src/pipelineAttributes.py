#!/bin/bash/python

from __future__ import print_function
import networkx as nx
from copy import deepcopy

import configurationClassErrors
from configurationClassErrors import *

import nodeAttributes
from nodeAttributes import *

import edgeAttributes
from edgeAttributes import *

import json
import os
import sys

class pipelineConfiguration:
  def __init__(self):
    self.configurationData = {}
    self.description       = 'No description provided'
    self.edgeMethods       = edgeClass()
    self.errors            = configurationClassErrors()
    self.filename          = ''
    self.instances         = {}
    self.nodeMethods       = nodeClass()
    self.nodeIDInteger     = 1
    self.pipelineName      = ''

  #TODO
  # Validate the contents of the tool configuration file.
  def processConfigurationData(self, data, filename):

    # First validate the contents of the data structure.
    success = self.validateConfigurationData(data)
    if not success: return False

    # Now put all of the data into data structures.
    self.configurationData = data
    self.instances         = data['instances']

  #TODO
  # Check that the pipeline configuration file is valid.  If so, put all the information in
  # the pipeline data structures.
  def validateConfigurationData(self, data):
    self.description = data['description']

    return True

  # Parse the pipeline configuration data and return a dictionary contaiing all of the tasks
  # appearing in the pipeline along with the tool required to perform the task.
  def getTasks(self):
    tasks = {}
    for task in self.configurationData['tasks']:
      tool        = self.configurationData['tasks'][task]['tool']
      tasks[task] = tool

    return tasks

  # Erase all of the data contained in the self.configurationData structure.
  def eraseConfigurationData(self):
    self.configurationData = {}
