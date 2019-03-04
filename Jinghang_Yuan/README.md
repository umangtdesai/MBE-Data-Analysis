# Project 1

## Datasets 

We have committed setup scripts for a MongoDB database that will set up the database and collection management functions that ensure users sharing the project data repository can read everyone's collections but can only write to their own collections. Once you have installed your MongoDB instance, you can prepare it by first starting `mongod` _without authentication_:
```
mongod --dbpath "<your_db_path>"
```
If you're setting up after previously running `setup.js`, you may want to reset (i.e., delete) the repository as follows.
```
mongo reset.js
```

### Working on data sets with authentication

