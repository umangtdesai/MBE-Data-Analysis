# Project 1 
### Gijung(Tony) Kim : tkixi@bu.edu

## Narrative
Tony has always preferred to ride bikes as his commute to class or to work.

As a biker, Tony would like to know what are some of the factors that may contribute to bike accidents. Especially in a city like Boston, it is critical to analyze data on bikes, the weather, traffic signal locations, hubway bike system, and on bike lanes.

Using the following 5 datasets, I computed complex transformations to produce 3 new datasets that would allow bikers to see their own safety in a new light.



## Datasets Used
Data Set|Description
-|-
Boston Hubway Station Location | A dataset provided by Hubway that shows all locations for Hubway bike stations in Boston.
Boston Collisions | A dataset provided by the team of the Vision Zero Boston program that contain the date, time, location, and type of incident
Boston Weather | A dataset provided by NOAA's National Centers for Environmental Information that shows the weather containing information such as precipitation, wind speed, and the date.
Boston Traffic Signal Locations | A dataset provided by the city of Boston's Analyze Boston that contains information on all of Boston's traffic signals
Boston Bike Lanes | A dataset provided by BostonGIS that contains information such as the jurisdiction, street, and the length of the bike lane
---

**This project description will be updated as we continue work on the infrastructure.**

## MongoDB infrastructure

### Setting up

We have committed setup scripts for a MongoDB database that will set up the database and collection management functions that ensure users sharing the project data repository can read everyone's collections but can only write to their own collections. Once you have installed your MongoDB instance, you can prepare it by first starting `mongod` _without authentication_:
```
mongod --dbpath "<your_db_path>"
```
If you're setting up after previously running `setup.js`, you may want to reset (i.e., delete) the repository as follows.
```
mongo reset.js
```
Next, make sure your user directories (e.g., `alice_bob` if Alice and Bob are working together on a team) are present in the same location as the `setup.js` script, open a separate terminal window, and run the script:
```
mongo setup.js
```
Your MongoDB instance should now be ready. Stop `mongod` and restart it, enabling authentication with the `--auth` option:
```
mongod --auth --dbpath "<your_db_path>"
```

### Working on data sets with authentication

With authentication enabled, you can start `mongo` on the repository (called `repo` by default) with your user credentials:
```
mongo repo -u alice_bob -p alice_bob --authenticationDatabase "repo"
```
However, you should be unable to create new collections using `db.createCollection()` in the default `repo` database created for this project:
```
> db.createCollection("EXAMPLE");
{
  "ok" : 0,
  "errmsg" : "not authorized on repo to execute command { create: \"EXAMPLE\" }",
  "code" : 13
}
```
Instead, load the server-side functions so that you can use the customized `createCollection()` function, which creates a collection that can be read by everyone but written only by you:
```
> db.loadServerScripts();
> var EXAMPLE = createCollection("EXAMPLE");
```
Notice that this function also prefixes the user name to the name of the collection (unless the prefix is already present in the name supplied to the function).
```
> EXAMPLE
alice_bob.EXAMPLE
> db.alice_bob.EXAMPLE.insert({value:123})
WriteResult({ "nInserted" : 1 })
> db.alice_bob.EXAMPLE.find()
{ "_id" : ObjectId("56b7adef3503ebd45080bd87"), "value" : 123 }
```
If you do not want to run `db.loadServerScripts()` every time you open a new terminal, you can use a `.mongorc.js` file in your home directory to store any commands or calls you want issued whenever you run `mongo`.

## Other required libraries and tools

You will need the latest versions of the PROV, DML, and Protoql Python libraries. If you have `pip` installed, the following should install the latest versions automatically:
```
pip install prov --upgrade --no-cache-dir
pip install dml --upgrade --no-cache-dir
pip install protoql --upgrade --no-cache-dir
```
If you are having trouble installing `lxml` in a Windows environment, you could try retrieving it [here](http://www.lfd.uci.edu/~gohlke/pythonlibs/).

Note that you may need to use `python -m pip install <library>` to avoid issues if you have multiple versions of `pip` and Python on your system.

## Formatting the `auth.json` file

The `auth.json` file should remain empty and should not be submitted. When you are running your algorithms, you should use the file to store your credentials for any third-party data resources, APIs, services, or repositories that you use. An example of the contents you might store in your `auth.json` file is as follows:
```
{
    "services": {
        "cityofbostondataportal": {
            "service": "https://data.cityofboston.gov/",
            "username": "alice_bob@example.org",
            "token": "XxXXXXxXxXxXxxXXXXxxXxXxX",
            "key": "xxXxXXXXXXxxXXXxXXXXXXxxXxxxxXXxXxxX"
        },
        "mbtadeveloperportal": {
            "service": "http://realtime.mbta.com/",
            "username": "alice_bob",
            "token": "XxXX-XXxxXXxXxXXxXxX_x",
            "key": "XxXX-XXxxXXxXxXXxXxx_x"
        }
    }
}
```
To access the contents of the `auth.json` file after you have loaded the `dml` library, use `dml.auth`.

## Running the execution script for a contributed project.

To execute all the algorithms for a particular contributor (e.g., `alice_bob`) in an order that respects their explicitly specified data flow dependencies, you can run the following from the root directory:
```
python execute.py alice_bob
```
To execute the algorithms for a particular contributor in trial mode, use the `-t` or `--trial` option:
```
python execute.py alice_bob --trial
