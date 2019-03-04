import dml
import datetime
import json
import math
import prov.model
import pprint
import uuid
from urllib.request import urlopen


class X(dml.Algorithm):
    # TODO 把X改成你的想要的名字，filename需要和classname一致

    contributor = "jshen97_leochans"
    reads = []
    writes = []
    # TODO 这里面写读取的data set和会产生的data set

    @staticmethod
    def execute(trial=False):

        start_time = datetime.datetime.now()

        # set up database connection
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('leochans_jshen97', 'leochans_jshen97')


        # TODO 我没理解错的话，简单的merge或combine都算？所以我前两个就都偷懒都是combine了


        repo.logout()

        end_time = datetime.datetime.now()

        return {"start": start_time, "end": end_time}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), start_time=None, end_time=None):

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('leochans_jshen97', 'leochans_jshen97')


        # TODO complete prov


        repo.logout()

        return doc

# debug
'''
X.execute()
doc = X.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''