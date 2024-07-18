
YouTube-Data-Harvesting-and-Warehousing-using-SQL-MongoDB-and-Streamlit.

Problem Statement: The problem statement is to create a Streamlit application that allows users to access and analyze data from multiple YouTube channels.

NAME : pONISHADEVI V

BATCH: DTM13

DOMAIN : DATA SCIENCE

Lanuage & Tool used: Python mongoDB pandas MYSQL
YouTube_Scrapping 
Introduction:✔✔
This project is a YouTube API scrapper that allows users to retrieve and analyze data from YouTube channels. It utilizes the YouTube Data API to fetch information such as channel , video details, comments, and more. The scrapper provides various functionalities to extract and process YouTube data for further analysis and insights


Technologies Used:
YouTube Data API: Utilizes the official YouTube Data API to interact with YouTube's platform and retrieve data.
Streamlit: The user interface and visualization are created using the Streamlit framework, providing a seamless and interactive experience.
MongoDB: The collected data can be stored in a MongoDB database for efficient data management and querying.
PyMongo: A Python library that enables interaction with MongoDB, a NoSQL database. It is used for storing and retrieving data from MongoDB in the YouTube Data Scraper.
Pandas: A powerful data manipulation and analysis library in Python. Pandas is used in the YouTube Data Scraper to handle and process data obtained from YouTube, providing functionalities such as data filtering, transformation, and aggregation.


Process Flow
Obtain YouTube API credentials: Visit the Google Cloud Console.

Create a new project or select an existing project.

Enable the YouTube Data API v3 for your project.

Create API credentials for youtube API v3.


ETL Process
Extracting Data from youtube API.

Transforming data into the required format.

Loading Data into SQL



Application Flow
Select Data Retrieval and Processing Page from dropdown menu at the sidebar.

Input the Channel Id and click on Get Channel Statistics in order to retrive data from Youtube API.

Next click on Push to MongoDB to store data in MongoDB Lake.

Select a channel name from the dropdown Channel Details and click on Push to SQL to import data into MYSQL.

Once imported, you can select the Analysis and Reports Page from the drop down to get a detailed analysis of the collected data.


License
The YouTube Data Scraper is released under the MIT License. Feel free to modify and use the code according to the terms of the license.

Conclusion
This YouTube API scrapper project aims to provide a powerful tool for retrieving, analyzing, and visualizing YouTube data, enabling users to gain valuable insights into channel performance, video engagement, and audience feedback.
