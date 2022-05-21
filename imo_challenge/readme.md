# IMO_challenge
## Coding Challenge from IMO

### Problem Statement:
#### IMO’s Search Product Team would like to know how often people are making different requests to their search engine.  
- They would like to know how many different searches are being requested. 
- The catch to this ask is that autosuggest searching may be enabled for some users; 
- this means that, for some users, a search is executed every time they make a keystroke.  
- IMO’s product team does not want to count these “suggestions” separately and would like them to only represent 1 search.  

#### Data Set details
- Every request has a user ID and a timestamp associated to it
- There are approximately 1,000,000 requests per day
- Not all users have autosuggest enabled.  Sadly, the product team forgot to log when this feature is turned on
- Data set lives in a flat file store and in a database (you may act as if this is an RDBMS or a NoSQL database)

#### Link for the Coding Challenge provided by IMO team [Link](https://github.com/ranjithpals/imo_challenge/blob/master/Data%20Engineer%20Technical%20round%20%20Interview.docx)


### Solution
#### Sample Input File
1. The sample file is created with the following fields
	- user_id
	- timestamp
	- search string
2. The data is a combination of following entries
	- users with single search entry
	- users with multiple search entries which are unique
	- users with multiple search entries with auto-suggest

#### Coding
1. Analytical data needed by the IMO team is provided through a PySpark based solution.
2. Sample file is stored in a Disk File System.
3. File is read and transformed using PySpark script.
4. Result data (Spark DataFrame) is stored onto DFS.
5. Unit test script is used to validate the functions from the PySpark script.
6. Unit test expected results declared as constants within the module, can be stored in a lookup file.

#### Scope for Code-Refactoring or Enchancing the existing solution.
1. Create a automated pipeline by Orchestrating the script run everyday using Apache Airflow or AWS managed Airflow service
2. Create a UDF which aids in the identifying whether a given entry in the log file is a auto-suggest or NOT, which replaces the few of the functions in the Module.
3. Create a Dashboard which monitor the health of the pipeline by capturing the 
	- number of users, total number of searches and mean, min and max number of searches per user everyday, weekly or monthly.
4. Create the pipeline in Cloud - preferably in AWS using
	- Ingest the daily logs (input) in S3.
	- Copy logs from S3 to Redshift to store the Raw input data (daily logs) in PostgreSQL tables.
	- Run the daily, weekly PySpark Script using EMR cluster
	- Copy data from cluster to Redshift to Store Output data in SQL tables (PostgreSQL).
	- Run Data Quality checks validating the Input and Output tables.
	- Orchestrate the above steps using Airflow.

	
