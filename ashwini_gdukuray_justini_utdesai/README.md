# CS504 Project

## Contributors
- Justin Ingwersen
- Ashwini Kulkarni
- Umang Desai
- Gahouray Dukuray

 ##Running the Code (and using the API)
 There are several steps in order to get the datasets loaded into mongodb and then to have access to the web service where
 the API is supplied. First, start your mongodb server. Next, cd into the root of this project directory and run the following command:
 
 ```python execute.py ashwini_gdukuray_justini_utdesai```
 
 Next, on a separate terminal (you should essentially have 3 open now!) cd into the folder 'ashwini_gdukuray_justini_utdesai'
 and then cd into the folder WebService. Here, run the following command:
 
  ```python app.py```
  
  Now the server is running. You can connect to the app via the browser using the url:
  
   ```http://localhost:5000/```
   
   Then, feel free to make your selection to load in a dataset using our API! The API will grab the data from the mongodb
   server running, and the datasets will be in there from the first command (execute.py) you ran. Enjoy!

### Datasets Used
The datasets we chose will be extremely valuable in order to help our partner learn more about the MBE world. They are as follows:
- MBE certification list from massHousing
- MBE certification list from the Secretary of Commonwealth
- Top companies with minority/women majority ownership
- Valid Zip Codes to highlight target areas
- Minority Business Development Agency firm and employee info

Our portals include:
- datamechanics.io
- cs-people.bu.edu
- mbda.gov/mbedata

 The datasets from massHousing and Secretary of Commonwealth are combined into a master list of MBE certified companies.
 This master list will allow many more transformations of data as well as provide a good place to lookup information 
 about specific businesses as needed. We will use the csv containing the top 25 companies with a minority or women 
 majority ownership to compare against the master list. This will show us what companies are top revenue producers as 
 well as MBE certified. It will also draw the question: why are the other top companies not certified in the first 
 place? In addition, we will be using the dataset from MBDA in order to visualize the number of total MBE firms in
 Massachusetts as well as number of employees working in these MBEs. We can also do a comparison against the rest of the
 United States.
 
 Lastly, we also will use a csv file containing a list of zip codes provided by our partner. These are the zip 
 codes we will focus on when analyzing MBE structure and the patterns of the companies. They represent the Greater 
 Boston Area.
 
 ### Non-Trivial Transformations Done/New Datasets Created
 - Our first major transformation was creating the masterList. This initially is a projection when converting the the
 csv file for the Secretary of Commonwealth dataset and the massHousing dataset to a JSON object to be inserted into
 the mongo database. Then we performed a selection of target zip codes, and finished with a union of them together,
 projecting out unwanted columns.
 - Another transformation was an aggregation. This started with taking the masterList and projecting the industries
 represented by the Description of Services. Then, we use an aggregation algorithm to group industries together and get
 the sum of each, resulting in a new dataset (in the form of a dataframe). This will help us see what industry is most
 common for MBEs.
 - Our final transformation involved the dataset containing the top 25 companies and the master list. To get the top 25
 companies, this involved a projection converting the csv file to JSON and inserted into the mongo database. Then, we 
 take the master list and merge it with this top company dataset performing a union. To first do this, we also had to
 transform the top company dataset and add a new column to represent a unique business ID that our master list uses. Our
 resulting data set contains 2 companies, which tells us they are the only two according to our master list that are MBE
 certified.
 
 ### Project 2 Justification (2b)
 For project 2, we combined suggestions from our professor and our partner and came up with an optimization/constraint
 problem that we can solve. We wanted to figure out which zip codes could best use an MBE injection into the area, primarily
 due to an overwhelming amount of non MBE businesses as well as a lack of a specific industry. This thus creates an opportunity
 for an MBE to come in and fill a niche role in this specific neighborhood. To do this, we used our master list of MBE businesses
 as well as our master list of non MBE businesses and merged the two together. We added back a column specifying their MBE
 status as this metadata is necessary to solve our constraint problem. We then employed a massive sorting algorithm to place
 each business in a respective industry 'bucket' as a way to standardize the industry that each company represents. With
 all 1000 companies sorted into their respective industries, we finally were able to run our optimization/constraint algorithm,
 located in optimalLocation.py, which broke our dataset into groups of datasets based on zip code. By doing this, this allows us
 to hone in on each area and attempt to add MBEs that fit our constraints of that area until there are no more MBEs left to add
 (by add, we mean get an MBE from a different zip code and 'move' them into the current zip code to fit a new role in a new area).
 The number of MBEs that we are able to add is the important metric that we track for each zip code and the algorithm emits
 a new dataset with each tuple being a zip code and the corresponding count of possible MBE additions. Thus, as a  result,
 we can clearly see which areas can best do with more MBE businesses!
 
 ### Project 2 Statistical Analysis (2c)
 In the file labeled correlationIndustries.py, we have completed some statistical analysis on the industries within each zipcode.
 We did this to better understand the relationships between industries. The metric we used is the correlation coefficient
 between two particular industries. This value tells us how correlated it is for two industries to be present in the same
 zip code (or absent). The file outputs a data set sorted by the correlation coefficient.
