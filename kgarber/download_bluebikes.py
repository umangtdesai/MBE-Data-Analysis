import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

# my imports
import csv
from tqdm import tqdm
from io import BytesIO, TextIOWrapper
from zipfile import ZipFile

# This algorithm downloads the hubway dataset and stores it in mongodb.

# This is somewhat challenging as the dataset is split into months and I want
# all of 2018, so I loop through the months and format the URL to get all of 
# the months.

# It's also challenging because the dataset is stored in "zip" format, so I need
# to unzip it in python, read the file from the archive, parse it (it's in CSV 
# format), then save the data in MongoDB.

# To make matters worse, in the middle of 2018 hubway changed their name to
# "bluebikes", so now I have some logic which handles that... we'll refer to 
# all the data as bluebikes

class download_bluebikes(dml.Algorithm):
    contributor = 'kgarber'
    reads = []
    writes = ['kgarber.bluebikes']
    # fields to cast to int or float after downloading them
    fields_to_int = [
        "tripduration", 
        "start station id", 
        "end station id", 
        "bikeid", 
        "birth year", 
        "gender",
    ]
    fields_to_float = [
        "start station latitude",
        "start station longitude",
        "end station latitude",
        "end station longitude",
    ]

    @staticmethod
    def execute(trial = False):
        print("Starting download_bluebikes algorithm.")
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('kgarber', 'kgarber')
        repo.dropCollection("bluebikes")
        repo.createCollection("bluebikes")

        # download the data for each month and insert it into MongoDB
        # https://s3.amazonaws.com/hubway-data/201812-hubway-tripdata.zip
        # [01, 02, 03, ... 10, 11, 12]
        # in month "05", the company name changed from hubway to bluebikes
        months = ["0" + str(i) if i < 10 else str(i) for i in range(1, 13)]
        name_from_month = lambda m: "hubway" if m < "05" else "bluebikes"
        month_tqdm = tqdm(months)
        for month in month_tqdm:
            company_name = name_from_month(month)
            month_tqdm.set_description("month %s - downloading" % month)
            url = "https://s3.amazonaws.com/hubway-data/2018%s-%s-tripdata.zip" % \
                (month, company_name)
            zip_urlopen = urllib.request.urlopen(url)
            # read the zip archive
            with ZipFile(BytesIO(zip_urlopen.read())) as current_zip_file:
                # we only have one file in the archive, get it
                filename = current_zip_file.namelist()[0]
                # open the file and parse the CSV
                with current_zip_file.open(filename) as opened_file:
                    current_csv = csv.DictReader(TextIOWrapper(opened_file))
                    # process the rows as necessary, this includes:
                    # 1) changing start time, stop time, and trip duration to timestamp
                    # 2) change start/end station ID to integer
                    # 3) change gender and birth year to int
                    # 4) change start/stop latitude/logitude to int
                    # additionally, build a list of dictionaries we can insert into mongodb
                    # also, for now, skip changing start and stop time so we dont' mess up time zones
                    result_rows = []
                    month_tqdm.set_description("month %s - parsing" % month)
                    for row_dict in current_csv:
                        row = {field: row_dict[field] for field in row_dict}
                        # format "2018-01-19 14:20:10"
                        # starttime = datetime.datetime.strptime(row["starttime"], "%Y-%m-%d %H:%M:%S").timestamp()
                        # cast strings to rows and ints
                        for field in download_bluebikes.fields_to_int:
                            # some birth years will be null, ignore it
                            if row[field] == "NULL":
                                if field != "birth year":
                                    print("caught null:", row)
                                row[field] = None
                            else:
                                row[field] = int(row[field])
                        for field in download_bluebikes.fields_to_float:
                            if row[field] == "NULL":
                                print("caught null:", row)
                                row[field] = None
                            else:
                                row[field] = float(row[field])
                        result_rows.append(row)
                    month_tqdm.set_description("month %s - inserting" % month)
                    repo['kgarber.bluebikes'].insert_many(result_rows)
                    repo['kgarber.bluebikes'].metadata({'complete':True})
        repo.logout()
        endTime = datetime.datetime.now()
        print("Finished download_bluebikes algorithm.")
        return {"start":startTime, "end":endTime}
    
    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        # our data mechanics class namespaces
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

        # the namespace for geospatial datasets in boston data portal
        doc.add_namespace('blb', 'https://www.bluebikes.com/system-data')

        # the agent which is my algorithn
        this_script = doc.agent(
            'alg:kgarber#download_bluebikes', 
            {
                prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 
                'ont:Extension':'py'
            })
        # the entity I am downloading
        resource = doc.entity(
            'blb:???',
            {
                'prov:label':'Bluebikes Dataset', 
                prov.model.PROV_TYPE:'ont:DataResource', 
                'ont:Extension':'csv'
            })
        # the activity of downloading this dataset (log the timing)
        get_bluebikes = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        # the activity is associated with the agent
        doc.wasAssociatedWith(get_bluebikes, this_script)
        # log an invocation of the activity
        doc.usage(get_bluebikes, resource, startTime, None,
            {
                prov.model.PROV_TYPE:'ont:Retrieval',
                'ont:Query':'https://s3.amazonaws.com/hubway-data/index.html'
            })
        # the newly generated entity
        bluebikes = doc.entity(
            'dat:kgarber#bluebikes', 
            {
                prov.model.PROV_LABEL:'Bluebikes', 
                prov.model.PROV_TYPE:'ont:DataSet'
            })
        # relations for the above entity
        doc.wasAttributedTo(bluebikes, this_script)
        doc.wasGeneratedBy(bluebikes, get_bluebikes, endTime)
        doc.wasDerivedFrom(bluebikes, resource, get_bluebikes, get_bluebikes, get_bluebikes)
        
        # return the generated provenance document
        return doc
