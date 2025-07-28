# Acquiring and Processing Information on the World's Largest Banks

In this project I analyse table under the heading By market capitalization from following web-page: 
https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks
I created a code that can be used to compile the list of the top 10 largest banks in the world 
ranked by market capitalization in billion USD. Further, the data needs to be transformed and stored in GBP,
EUR and INR as well, in accordance with the exchange rate information that has been made available to you as a CSV file. 
The processed information table is to be saved locally in a CSV format and as a database table.

## Features

- Extract the tabular information from the given URL under the heading 'By market capitalization' and save it to a dataframe.
- Transform the dataframe by adding columns for Market Capitalization in GBP, EUR and INR, rounded to 2 decimal places,
  based on the exchange rate information shared as a CSV file.
- Load the transformed dataframe to an output CSV file.
- Load the transformed dataframe to an SQL database server as a table.
- Run queries on the database table.
- log the progress of the code at different stages in a file code_log.txt.

## Technologies Used

- Python core
- Python libraries(pandas, numpy)
- request and BeautifulSoup libraries for data extraction from web-page 
- SQLite

## How to Run and how to see what does this code does

you can use any IDE that supports Python programming language, clone this repository,
delete some files, leave just two following: "banks_project.py" and "exchange_rate.csv",
make sure that all libraries imported in "banks_project.py" is installed in your IDE,
run "banks_project.py" file and all deleted files will be created automatically,
you can see and analyse "Largest_banks_data.csv" and "code_log.txt" files
and also information shown in CLI by querying table from "Banks.db" SQLite database.


