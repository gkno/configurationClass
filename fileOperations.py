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
  def readConfigurationFile(self, filename, allowTermination = True):
    try: jsonData = open(filename)
    except:
      if allowTermination: self.errors.missingFile(filename)
      else: return False

    try: configurationData = json.load(jsonData)
    except:
      if allowTermination:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        self.errors.jsonError(exc_value, filename)
      else: return False

    return configurationData
