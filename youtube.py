from googleapiclient.discovery import build
import pymongo
import pymysql
import pandas as pd
from datetime import datetime
import streamlit as st


#API key connection

def Api_connect():
    api_key="AIzaSyAK-moLhpW2U6_GAAhFhlh6GAtr71s_uZI"
    api_service_name = "youtube"
    api_version = "v3"
    youtube =build(api_service_name, api_version, developerKey=api_key)
    return youtube 
youtube=Api_connect()



# GET CHANNEL DETAILS
def get_channel_info(channel_id):
    request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=channel_id
        )
    response= request.execute()

    for i in response['items']:
        DATA=dict(channel_id=i['id'],
                  channel_name=i['snippet']['title'],
                  channel_description=i['snippet']['description'],
                  channel_views=i['statistics']['viewCount'],
                  subscrition_count=i['statistics']['subscriberCount'],
                  video_count=i['statistics']['videoCount'],
                  channel_playlist=i['contentDetails']['relatedPlaylists']['uploads']
                  )
    return DATA   




#GET VIDEO IDS
def get_videos_ids(channel_id):
    video_ids=[]
    request= youtube.channels().list(id=channel_id,
                    part="contentDetails")
    response=request.execute()

    playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None

    while True:


        request = youtube.playlistItems().list(
                                                part='snippet',
                                        
                                                playlistId= playlist_id,
                                                maxResults=50,
                                                pageToken=next_page_token
                                                )
                    
        response1 = request.execute()
        for i in range(len(response1['items'])):

            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get("nextPageToken")

        if next_page_token is None:
            break

    return video_ids    


# GET VIDEOS DETAILS

def get_video_info(VIDEO_IDS):
    video_data=[]
    for video_id in VIDEO_IDS:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        video_response = request.execute()

        for i in video_response['items']:
            DATA=dict(channel_name=i['snippet']['channelTitle'],
                    channel_id=i['snippet']['channelId'],
                    video_id=i['id'],
                    video_name=i['snippet']['title'],
                    video_description=i['snippet'].get('description'),
                    Tags=','.join(i['snippet'].get('tags',['NA'])),
                    Thumbnail=i['snippet']['thumbnails']['default']['url'],
                    PublishedAt=i['snippet']['publishedAt'],
                    view_count=i['statistics'].get('viewCount'),
                    like_count=i['statistics'].get('likeCount'),
                    favorite_count=i['statistics']['favoriteCount'],
                    comment_count=i['statistics'].get('commentCount'),
                    Duration=i['contentDetails']['duration'],
                    Definition=i['contentDetails']['definition'],
                    Caption_status=i['contentDetails']['caption']
                )
            video_data.append(DATA)
    return video_data        
    

#GET COMMENTS DETAILS

def get_comment_info(VIDEO_IDS):
    comment_data=[]
    try:
        for video_id in VIDEO_IDS:
            request = youtube.commentThreads().list(
                    part="snippet",
                    videoId=video_id,
                    maxResults=50
                )
            comments_response = request.execute()

            for i in comments_response['items']:
                DATA=dict(comment_id=i['snippet']['topLevelComment']['id'],
                        video_id=i['snippet']['topLevelComment']['snippet']['videoId'],
                        comment_text=i['snippet']['topLevelComment']['snippet']['textDisplay'],
                        comment_author=i['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        comment_publishedAt=i['snippet']['topLevelComment']['snippet']['publishedAt']
                        )
                
                comment_data.append(DATA)
    except:
        pass
    return comment_data     


#GET PLAYLIST DETAILS
def get_playlist_detail(channel_id):
        next_page_token=None
        PLAYLIST_DATA=[]

        while True:

                request = youtube.playlists().list(
                        part='snippet,contentDetails',
                        channelId=channel_id,
                        maxResults=50,
                        pageToken=next_page_token  
                    )
                response = request.execute()

                for i in response['items']:
                        DATA=dict(playlist_id=i['id'],
                                Title=i['snippet']['title'],
                                channel_id=i['snippet']['channelId'],
                                channel_name=i['snippet']['channelTitle'],
                                publishedAt=i['snippet']['publishedAt'],
                                item_count=i['contentDetails']['itemCount']
                                )
                        PLAYLIST_DATA.append(DATA)


                next_page_token=response.get('nextPageToken')
                if next_page_token is None:
                    break
        return PLAYLIST_DATA



#UPLOAD TO MONGODB
client=pymongo.MongoClient('mongodb://localhost:27017/')
mydb=client["YOUTUBE_DATA"]


def channel_information(channel_id):
    CHANNEL_DATA=get_channel_info(channel_id)
    PLAYLIST_DATA=get_playlist_detail(channel_id)
    VIDEO_IDS=get_videos_ids(channel_id)
    VIDEO_DATA=get_video_info(VIDEO_IDS)
    COMMENT_DATA=get_comment_info(VIDEO_IDS)
    
    information=mydb.channel_information

    information.insert_one({"channel_details":CHANNEL_DATA,"playlist_details":PLAYLIST_DATA,
                        "video_details":VIDEO_DATA,"comments_details":COMMENT_DATA})
    
    return "upload successfully"



#create channels_data table in mysql insert values
def channels_table():
    pysql_connect=pymysql.connect(host='127.0.0.1',user='root',password='new_password',database="youtube_data")
    cur=pysql_connect.cursor()


    drop_query="""drop table if exists channel_data"""
    cur.execute(drop_query)
    pysql_connect.commit()

    try:
        CHANNEL=""" CREATE TABLE IF NOT EXISTS channel_data(channel_id varchar(255) primary key , channel_name varchar(255), channel_description text, channel_views bigint, subscrition_count bigint, video_count int, channel_playlist varchar(100)) """
        cur.execute(CHANNEL)
        pysql_connect.commit()
    except:
        print("channels table already create")  
    

    #python to mongodb connection
    # get details for channel in channel_informatio in mongodb
        
    channels_collection=[]
    client=pymongo.MongoClient('mongodb://localhost:27017/')
    mydb=client["YOUTUBE_DATA"]
    information=mydb.channel_information
    for i in information.find({},{"_id":0,"channel_details":1}):
        channels_collection.append(i["channel_details"])
    df=pd.DataFrame(channels_collection)  



    # insert value into sql table channel_data
    sql="insert into channel_data(channel_id,channel_name,channel_description,channel_views,subscrition_count,video_count,channel_playlist) values (%s,%s,%s,%s,%s,%s,%s)"
    for i in range(0,len(df)):
        try:    
            cur.execute(sql,tuple(df.iloc[i]))
            pysql_connect.commit()
        except:
            print("channel_data values are already inserted") 


    

#create playlist_data table in mysql insert values        
def playlist_table():
    pysql_connect=pymysql.connect(host='127.0.0.1',user='root',password='new_password',database="youtube_data")
    cur=pysql_connect.cursor()


    drop_query="""drop table if exists playlists_data"""
    cur.execute(drop_query)
    pysql_connect.commit()

    create_query=""" CREATE TABLE IF NOT EXISTS playlists_data( playlist_id varchar(100) primary key,Title text,channel_id text ,channel_name text, publishedAt varchar(100) , item_count int) """
    cur.execute(create_query)
    pysql_connect.commit()  
    



    playlist_collection=[]
    client=pymongo.MongoClient('mongodb://localhost:27017/')
    mydb=client["YOUTUBE_DATA"]
    information=mydb.channel_information
    for pl_data in information.find({},{"_id":0,"playlist_details":1}):
        for i in range(len(pl_data["playlist_details"])):
            playlist_collection.append(pl_data["playlist_details"][i])
    df1=pd.DataFrame(playlist_collection)
     
  
    



    # insert value into sql table playlist_data
    sql='''insert into playlists_data(playlist_id ,Title,channel_id,channel_name, publishedAt, item_count) values (%s,%s,%s,%s,%s,%s)'''
    for i in range(0,len(df1)):
            cur.execute(sql,tuple(df1.iloc[i]))
            pysql_connect.commit()







   


#create video_data table in mysql insert values
def videos_table():
    pysql_connect=pymysql.connect(host='127.0.0.1',user='root',password='new_password',database="youtube_data")
    cur=pysql_connect.cursor()


    drop_query="""drop table if exists videos_data"""
    cur.execute(drop_query)
    pysql_connect.commit()


    create_query="""CREATE TABLE IF NOT EXISTS videos_data( 
                                                            channel_name varchar(255),
                                                            channel_id varchar(100),
                                                            video_id varchar(100) primary key,
                                                            video_name varchar(150),
                                                            video_description text,
                                                            Tags text,
                                                            Thumbnail varchar(200),
                                                            PublishedAt varchar(100),
                                                            view_count bigint,
                                                            like_count bigint,
                                                            favorite_count int,
                                                            comment_count int,
                                                            Duration varchar(20),
                                                            Definition varchar(50),
                                                            Caption_status varchar(20)
                                                            )"""
    cur.execute(create_query)
    pysql_connect.commit()  

    video_collection=[]
    client=pymongo.MongoClient('mongodb://localhost:27017/')
    mydb=client["YOUTUBE_DATA"]
    information=mydb.channel_information
    for vi_data in information.find({},{"_id":0,"video_details":1}):
        for i in range(len(vi_data["video_details"])):
            video_collection.append(vi_data["video_details"][i])
    df2=pd.DataFrame(video_collection)
    # insert value into sql table videos_data


    sql = '''INSERT INTO videos_data(
        channel_name, channel_id, video_id, video_name, video_description, Tags, Thumbnail, PublishedAt,
        view_count, like_count, favorite_count, comment_count, Duration, Definition, Caption_status
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''

    for i in range(0,len(df2)):
        cur.execute(sql,tuple(df2.iloc[i]))
        pysql_connect.commit()





## comment information are insert into mysql
def comments_table():
    pysql_connect=pymysql.connect(host='127.0.0.1',user='root',password='new_password',database="youtube_data")
    cur=pysql_connect.cursor()


    drop_query="""drop table if exists comments_data"""
    cur.execute(drop_query)
    pysql_connect.commit()


    create_query=""" CREATE TABLE IF NOT EXISTS comments_data(comment_id varchar(100) primary key,
                                                            video_id varchar(50),
                                                            comment_text text,
                                                            comment_author varchar(150),                  
                                                            comment_publishedAt varchar(50)) """

    cur.execute(create_query)
    pysql_connect.commit()  


    comments_collection=[]
    client=pymongo.MongoClient('mongodb://localhost:27017/')
    mydb=client["YOUTUBE_DATA"]
    information=mydb.channel_information
    for com_data in information.find({},{"_id":0,"comments_details":1}):
        for i in range(len(com_data["comments_details"])):
            comments_collection.append(com_data["comments_details"][i])
    df3=pd.DataFrame(comments_collection)
    


    sql='''insert into comments_data(comment_id ,video_id,comment_text ,comment_author,comment_publishedAt) values(%s,%s,%s,%s,%s)'''
    for i in range(0,len(df3)):
                cur.execute(sql,tuple(df3.iloc[i]))
                pysql_connect.commit()  




def tables():
    channels_table()
    videos_table()
    playlist_table()
    comments_table()


    return "Tables Created Successfully"   



def show_channels_table():
    channels_collection=[]
    client=pymongo.MongoClient('mongodb://localhost:27017/')
    mydb=client["YOUTUBE_DATA"]
    information=mydb.channel_information
    for i in information.find({},{"_id":0,"channel_details":1}):
        channels_collection.append(i["channel_details"])
    df=st.dataframe(channels_collection) 
    return df 


def show_playlists_table():
    playlist_collection=[]
    client=pymongo.MongoClient('mongodb://localhost:27017/')
    mydb=client["YOUTUBE_DATA"]
    information=mydb.channel_information
    for pl_data in information.find({},{"_id":0,"playlist_details":1}):
        for i in range(len(pl_data["playlist_details"])):
            playlist_collection.append(pl_data["playlist_details"][i])
    df1=st.dataframe(playlist_collection)
    return df1


def show_videos_table():   
    video_collection=[]
    client=pymongo.MongoClient('mongodb://localhost:27017/')
    mydb=client["YOUTUBE_DATA"]
    information=mydb.channel_information
    for vi_data in information.find({},{"_id":0,"video_details":1}):
        for i in range(len(vi_data["video_details"])):
            video_collection.append(vi_data["video_details"][i])
    df2=st.dataframe(video_collection)
    return df2



def show_comments_table():
    comments_collection=[]
    client=pymongo.MongoClient('mongodb://localhost:27017/')
    mydb=client["YOUTUBE_DATA"]
    information=mydb.channel_information
    for com_data in information.find({},{"_id":0,"comments_details":1}):
        for i in range(len(com_data["comments_details"])):
            comments_collection.append(com_data["comments_details"][i])
    df3=st.dataframe(comments_collection)

    return df3



#streamlit part


with st.sidebar:
    st.markdown('''WELCOME TO STREAMLIT! :balloon:''')
    st.title(":Green[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("Dream big, Believe bold!")
    st.caption("Python Scripting")
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption("Data Management using MongoDB and SQL")


channel_id=st.text_input("Enter the channel ID")

if st.button("collect and store data"):

    chl_ids=[]
    mydb=client["YOUTUBE_DATA"]
    information=mydb.channel_information
    for i in information.find({},{"_id":0,"channel_details":1}):
        chl_ids.append(i['channel_details']['channel_id'])

    if channel_id in chl_ids:
        st.success("Channel information of the given channel id already exists")

    else:
        output=channel_information(channel_id)
        st.success(output)


if st.button("Migrate to Sql"):
    Tables=tables()
    st.success(Tables)


show_table=st.radio( "SELECT THE TABLE FOR VIEW",(":green[CHANNELS]",":orange[PLAYLISTS]",":red[VIDEOS]",":blue[COMMENTS]"))
if show_table == ":green[CHANNELS]":
    show_channels_table()

elif show_table == ":orange[PLAYLISTS]":
    show_playlists_table()

elif show_table ==":red[VIDEOS]":
    show_videos_table()

elif show_table == ":blue[COMMENTS]":
    show_comments_table()              



#SQL CONNECTION
pysql_connect=pymysql.connect(host='127.0.0.1',user='root',password='new_password',database="youtube_data")
cur=pysql_connect.cursor()


Question=st.selectbox("Select your question" ,("1. All the videos and the channel name",
                                               "2. Channels with most number of videos",
                                               "3. 10 most viewed videos",
                                               "4. Comments in each videos",
                                               "5. Videos with highest likes",
                                               "6. likes of all videos",
                                               "7. views of each channel",
                                               "8. videos published in the year of 2022",
                                               "9. average duration of all videos in each channel",
                                               "10. videos with highest number of comments")) 



if Question=="1. All the videos and the channel name":
    Query1=""" select video_name as videos,channel_name as channelname from videos_data"""
    cur.execute(Query1)
    pysql_connect.commit()
    t1=cur.fetchall()
    df=pd.DataFrame(t1,columns=["Video name","Channel name"])
    st.write(df)

elif Question=="2. Channels with most number of videos":
    Query2=""" select channel_name as channelname,video_count as total_videos from channel_data 
                order by video_count desc  """
    cur.execute(Query2)
    pysql_connect.commit()
    t2=cur.fetchall()
    df2=pd.DataFrame(t2,columns=["Channel name","Total videos"])
    st.write(df2)


elif Question=="3. 10 most viewed videos":
    Query3=""" select view_count ,channel_name,video_name from videos_data
                where view_count is not null order by view_count desc limit 10"""
    cur.execute(Query3)
    pysql_connect.commit()
    t3=cur.fetchall()
    df3=pd.DataFrame(t3,columns=["Views","Channel name","Video name"])
    st.write(df3)

elif Question=="4. Comments in each videos":
    Query4=""" select comment_count,video_name from videos_data where comment_count is not null"""
    cur.execute(Query4)
    pysql_connect.commit()
    t4=cur.fetchall()
    df4=pd.DataFrame(t4,columns=["Comment count","Video name"])
    st.write(df4)    


elif Question=="5. Videos with highest likes":
    Query5=""" select video_name ,channel_name,like_count from videos_data 
                where like_count is not null order by like_count desc"""
    cur.execute(Query5)
    pysql_connect.commit()
    t5=cur.fetchall()
    df5=pd.DataFrame(t5,columns=["Video name","Channel name","likecount"])
    st.write(df5)


elif Question=="6. likes of all videos":
    Query6=""" select video_name,like_count from videos_data  """
    cur.execute(Query6)
    pysql_connect.commit()
    t6=cur.fetchall()
    df6=pd.DataFrame(t6,columns=["Video name","likecount"])
    st.write(df6)


elif Question=="7. views of each channel":
    Query7=""" select channel_name,channel_views from channel_data  """
    cur.execute(Query7)
    pysql_connect.commit()
    t7=cur.fetchall()
    df7=pd.DataFrame(t7,columns=["Channel name","Channel views"])
    st.write(df7)

elif Question=="8. videos published in the year of 2022":
    Query8=""" select video_name,CONCAT(DATE(PublishedAt), ' ', TIME(PublishedAt)) AS PublishedDate,channel_name from videos_data 
                where extract(year from PublishedAt)=2022 """
    cur.execute(Query8)
    pysql_connect.commit()
    t8=cur.fetchall()
    df8=pd.DataFrame(t8,columns=["Videos name","Published_Date","Channel name"])
    st.write(df8)   

elif Question=="9. average duration of all videos in each channel":
    Query9=""" select channel_name,sec_to_time(AVG(time_to_sec(str_to_date(Duration,'PT%iM%sS')))) as average_Duration
    from videos_data group by channel_name"""
    cur.execute(Query9)
    pysql_connect.commit()
    t9=cur.fetchall()
    df9=pd.DataFrame(t9,columns=["Channel name","Average Duration"])
    

    T9=[]
    for index,row in df9.iterrows():
        channel_title=row["Channel name"]
        average_Duration=row["Average Duration"]
        average_Duration_str=str(average_Duration) 
        T9.append(dict(channelname=channel_title,Averageduration=average_Duration_str))
    df1=pd.DataFrame(T9)
    st.write(df1)
    



elif Question=="10. videos with highest number of comments":
    Query10=""" select video_name,channel_name,comment_count from videos_data 
            where comment_count is not null order by comment_count desc"""
    cur.execute(Query10)
    pysql_connect.commit()
    t10=cur.fetchall()
    df10=pd.DataFrame(t10,columns=["Video name","Channel name","Comment count"])
    st.write(df10)
    