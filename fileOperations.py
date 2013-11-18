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

class fileOperations:
  def __init__(self):
    self.errors = configurationClassErrors()

  # Open a configuration file and store the contents of the file in the
  # configuration dictionary.
  def readConfigurationFile(self, filename):
    try: jsonData = open(filename)
    except: self.errors.missingFile(filename)

    try: configurationData = json.load(jsonData)
    except:
      exc_type, exc_value, exc_traceback = sys.exc_info()
      self.errors.jsonError(exc_value, filename)

    return configurationData
