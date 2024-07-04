from googleapiclient.discovery import build
import mysql.connector
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import streamlit as st

# SQL Connection
mydb = mysql.connector.connect(
    host="localhost",
    port="3306",
    user="root",
    password="admin@123",
    database="ytdata"
)

cursor = mydb.cursor()

# API key Connection
def api_connection():
    api_key = "AIzaSyBkJZSRN3M4BKVO2m17edCaL6D6zy_1Dak"
    api_service_name = "youtube"
    api_version = "v3"

    youtube = build(api_service_name, api_version, developerKey=api_key)
    return youtube

yt_call = api_connection()

# Get Channel Information
def Channel_Info(channel_id):
    cursor.execute("""CREATE TABLE IF NOT EXISTS channel_info (
                        channel_name VARCHAR(255),
                        channel_id VARCHAR(255),
                        subscribe INT,
                        views INT,
                        total_videos INT,
                        channel_description TEXT
                    )""")
    
    request = yt_call.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute()
    
    for item in response.get('items', []):
        details = dict(Channel_Name=item['snippet']['title'],
                       Channel_Id=item['id'],
                       Subscribers=item['statistics']['subscriberCount'],
                       Views=item['statistics']['viewCount'],
                       Total_Videos=item['statistics']['videoCount'],
                       Channel_Description=item['snippet']['description'])
    
        cursor.execute("INSERT INTO channel_info (channel_name, channel_id, subscribe, views, total_videos, channel_description) VALUES (%s, %s, %s, %s, %s, %s)",
                       (details['Channel_Name'], 
                        details['Channel_Id'], 
                        details['Subscribers'], 
                        details['Views'], 
                        details['Total_Videos'], 
                        details['Channel_Description']))
        
        mydb.commit()
    return details

# Get Video Id
def Get_Video_Id(video_id):
    Video_ID = []
    response = yt_call.channels().list(id=video_id,
                                       part='contentDetails').execute()

    Playlist_ID = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    Next_Page_Token = None

    while True:
        request_1 = yt_call.playlistItems().list(
            part='snippet',
            playlistId=Playlist_ID,
            maxResults=50,
            pageToken=Next_Page_Token).execute()
        for i in range(len(request_1['items'])):
            Video_ID.append(request_1['items'][i]['snippet']['resourceId']['videoId'])
        Next_Page_Token = request_1.get('nextPageToken')

        if Next_Page_Token is None:
            break
      
    return Video_ID

# Get Video Details
def parse_duration(duration_str):
    try:
        duration_seconds = int(duration_str[2:-1])
        return duration_seconds
    except ValueError:
        return None

def Get_Video_Details(Video_id):
    Video_List = []
    for v_id in Video_id:
        request = yt_call.videos().list(
            part="snippet,contentDetails,statistics",
            id=v_id
        )
        response = request.execute()

        cursor.execute("""CREATE TABLE IF NOT EXISTS video_details(
                    channel_name VARCHAR(255),
                    channel_id VARCHAR(255),
                    video_id VARCHAR(255),
                    title TEXT,
                    tags TEXT,
                    thumbnail TEXT,
                    description TEXT,
                    published_date DATETIME,
                    duration TIME,
                    views INT,
                    likes INT,
                    dislikes INT,
                    comments INT
                )""")

        for item in response['items']:
            Data = dict(
                channel_Name=item['snippet']['channelTitle'],
                Channel_Id=item['snippet']['channelId'],
                Video_Id=item['id'],
                Title=item['snippet']['title'],
                Tags=json.dumps(item.get('tags')),
                Thumbnail=json.dumps(item['snippet']['thumbnails']),
                Description=item['snippet'].get('description', ''),
                Publish_Date=item['snippet']['publishedAt'],
                Duration=item['contentDetails']['duration'],
                Views=item['statistics'].get('viewCount', 0),
                Likes=item['statistics'].get('likeCount', 0),
                Dislikes=item['statistics'].get('dislikeCount'),
                Comments=item['statistics'].get('commentCount', 0)
            )
            
            Video_List.append(Data)

            duration_seconds = parse_duration(Data['Duration'])
            if duration_seconds is not None:
               duration = timedelta(seconds=duration_seconds)
            else:
               duration = timedelta(seconds=0) 
            current_datetime = datetime.now()
            updated_datetime = current_datetime + duration
            sql_duration = updated_datetime.strftime('%Y-%m-%d %H:%M:%S')

            iso_datetime = Data['Publish_Date']
            parsed_datetime = datetime.fromisoformat(iso_datetime.replace('Z', '+00:00'))
            mysql_published_date = parsed_datetime.strftime('%Y-%m-%d %H:%M:%S')

            cursor.execute("INSERT INTO video_details(channel_name, channel_id, video_id, title, tags, thumbnail, description, published_date, duration, views, likes, dislikes, comments) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s)",
                           (Data['channel_Name'], 
                            Data['Channel_Id'], 
                            Data['Video_Id'], 
                            Data['Title'], 
                            Data['Tags'], 
                            Data['Thumbnail'], 
                            Data['Description'], 
                            mysql_published_date, 
                            sql_duration, Data['Views'], 
                            Data['Likes'], 
                            Data['Dislikes'], 
                            Data['Comments']))

            mydb.commit()        
    return Video_List

# Get Comment Details
def get_comment_Details(get_Comment):
    comment_List = []
    try:
        cursor.execute("""CREATE TABLE IF NOT EXISTS comment_details (
                            comment_id VARCHAR(255),
                            video_id VARCHAR(255),
                            comment_text TEXT,
                            author VARCHAR(255),
                            published_date DATETIME
                        )""")
        for Com_Det in get_Comment:
            request = yt_call.commentThreads().list(
                part="snippet",
                videoId=Com_Det,
                maxResults=50
            )
            response = request.execute()

            for item in response['items']:
                Comment_Det = dict(Comment_ID=item['snippet']['topLevelComment']['id'],
                                   Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                                   Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                                   Author_Name=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                   Published_Date=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                    
                comment_List.append(Comment_Det)

                iso_datetime = Comment_Det['Published_Date']
                parsed_datetime = datetime.fromisoformat(iso_datetime.replace('Z', '+00:00'))
                mysql_published_dates = parsed_datetime.strftime('%Y-%m-%d %H:%M:%S')

                cursor.execute("INSERT INTO comment_details (comment_id, video_id, comment_text, author, published_date) VALUES (%s, %s, %s, %s,%s)",
                               (Comment_Det['Comment_ID'], 
                                Comment_Det['Video_Id'], 
                                Comment_Det['Comment_Text'], 
                                Comment_Det['Author_Name'], 
                                mysql_published_dates))
                mydb.commit()
                    
    except Exception as e:
        print(f"Error: {e}")
         
    return comment_List

# Overall Function to get details
def fetch_all_data(channel_id):
    channel_info = Channel_Info(channel_id)
    video_ids = Get_Video_Id(channel_id)
    video_details = Get_Video_Details(video_ids)
    comment_details = get_comment_Details(video_ids)

    df_channel_info = pd.DataFrame([channel_info])
    df_video_details = pd.DataFrame(video_details)
    df_comment_details = pd.DataFrame(comment_details)

    return df_channel_info, df_video_details, df_comment_details

# Main Function
def main():
    st.title("YouTube Data Harvesting and Warehousing")

    menu = ["Home", "Queries"]
    choice = st.sidebar.selectbox("Menu", menu)

    if choice == "Home":
        st.subheader("Home")
        channel_id = st.text_input("Enter Channel ID")
        if st.button("Fetch Details"):
            df_channel_info, df_video_details, df_comment_details = fetch_all_data(channel_id)
            st.success("Data Harvested Successfully")
            st.write("Channel Info")
            st.dataframe(df_channel_info)
            st.write("Video Details")
            st.dataframe(df_video_details)
            st.write("Comment Details")
            st.dataframe(df_comment_details)

    elif choice == "Queries":
        st.subheader("Queries")
        query = st.selectbox("Select Query", [
            "1. What are the names of all the videos and their corresponding channels?",
            "2. Which top 10 videos have the highest number of likes and what are their corresponding channel names?",
            "3. What are the top 10 most viewed videos and their respective channel names?",
            "4. How many comments were made on each video, and what are their corresponding video names?",
            "5. Which videos have the highest number of comments and what are their corresponding channel names?",
            "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
            "7. What is the total number of views for each channel, and what are their corresponding channel names?",
            "8. What are the names of all the channels that have published videos in the year 2022?",
            "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
            "10. Which videos have the highest number of comments, and what are their corresponding channel names?"
        ])

        if st.button("Run Query"):
            if query == "1. What are the names of all the videos and their corresponding channels?":
                cursor.execute("SELECT title, channel_name FROM video_details")
                results = cursor.fetchall()
                df = pd.DataFrame(results, columns=['Video Title', 'Channel Name'])
                st.dataframe(df)

            elif query == "2. Which top 10 videos have the highest number of likes and what are their corresponding channel names?":
                cursor.execute("SELECT title, channel_name, likes FROM video_details ORDER BY likes DESC LIMIT 10")
                results = cursor.fetchall()
                df = pd.DataFrame(results, columns=['Video Title', 'Channel Name', 'Likes'])
                st.dataframe(df)

            elif query == "3. What are the top 10 most viewed videos and their respective channel names?":
                cursor.execute("SELECT title, channel_name, views FROM video_details ORDER BY views DESC LIMIT 10")
                results = cursor.fetchall()
                df = pd.DataFrame(results, columns=['Video Title', 'Channel Name', 'Views'])
                st.dataframe(df)

            elif query == "4. How many comments were made on each video, and what are their corresponding video names?":
                cursor.execute("SELECT title, comments FROM video_details")
                results = cursor.fetchall()
                df = pd.DataFrame(results, columns=['Video Title', 'Comments'])
                st.dataframe(df)

            elif query == "5. Which videos have the highest number of comments and what are their corresponding channel names?":
                cursor.execute("SELECT title, channel_name, comments FROM video_details ORDER BY comments DESC LIMIT 10")
                results = cursor.fetchall()
                df = pd.DataFrame(results, columns=['Video Title', 'Channel Name', 'Comments'])
                st.dataframe(df)
                
            elif query == "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
                cursor.execute("SELECT title, likes, dislikes FROM video_details")
                results = cursor.fetchall()
                df = pd.DataFrame(results, columns=['Video Title', 'Likes', 'Dislikes'])
                st.dataframe(df)

            elif query == "7. What is the total number of views for each channel, and what are their corresponding channel names?":
                cursor.execute("SELECT channel_name, SUM(views) as total_views FROM video_details GROUP BY channel_name")
                results = cursor.fetchall()
                df = pd.DataFrame(results, columns=['Channel Name', 'Total Views'])
                st.dataframe(df)

            elif query == "8. What are the names of all the channels that have published videos in the year 2022?":
                cursor.execute("SELECT DISTINCT channel_name FROM video_details WHERE YEAR(published_date) = 2022")
                results = cursor.fetchall()
                df = pd.DataFrame(results, columns=['Channel Name'])
                st.dataframe(df)

            elif query == "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?":
                cursor.execute("SELECT channel_name, AVG(duration) as avg_duration FROM video_details GROUP BY channel_name")
                results = cursor.fetchall()
                df = pd.DataFrame(results, columns=['Channel Name', 'Average Duration'])
                st.dataframe(df)

            elif query == "10. Which videos have the highest number of comments, and what are their corresponding channel names?":
                cursor.execute("SELECT title, channel_name, comments FROM video_details ORDER BY comments DESC LIMIT 10")
                results = cursor.fetchall()
                df = pd.DataFrame(results, columns=['Video Title', 'Channel Name', 'Comments'])
                st.dataframe(df)

if __name__ == '__main__':
    main()

cursor.close()
mydb.close()
