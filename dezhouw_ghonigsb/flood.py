# flood.py
# created on Feb 27, 2019

import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class flood(dml.Algorithm):
	contributor = "dezhouw_ghonigsb"
	reads.      = []
	writes.     = []

	@staticmethod
	def execute(trial = False):
		pass

	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
		pass
