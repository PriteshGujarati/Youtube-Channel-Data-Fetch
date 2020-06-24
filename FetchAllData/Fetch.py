# install below packages in python 
"""
pip install isodate
pip install textblob
pip install youtube-data-api

"""

import pandas as pd 
import requests 
import dateutil.parser
import isodate
from textblob import TextBlob
from apiclient.discovery import build
from key_file import api_key 

youtube = build('youtube', 'v3', developerKey = api_key)
c1 = ['students', 'gre' , 'ielts' , 'tofel' ,'score', 'sop' , 'lor' , 'masters' ,'fall', 'spring', 'student','scholarships', 'scholarship' ]
c2 =  ['life','living' , 'california' , 'vlog' , 'shopping' , 'home' , 'house' , 'mom' , 'wife','singing', 'kaitlyn' , 'marriage','town' , 'lockdown','sang','bollywood','family!','blackmailed']
c3 =  [' q&a', 'questions', 'answers' , 'doubts' , 'problems ','immigration','rule', 'trump','president']
c4 =  ['linkedin','job', 'jobs' , 'career', 'interview', 'interviews' , 'companies', 'company' , 'resume' , 'portfolio' , 'developer', 'skills' , 'Manager']

def mainfetch(cid):
    print("in mainfetch")
    videos = get_channel_videos(cid)
    video_ids = list(map(lambda x:x['snippet']['resourceId']['videoId'],videos))
    durations = get_video_duration(video_ids)
    
    IdDuration = []
    for duration in durations:
        IdDuration.append([duration['id'],converttosec(duration['contentDetails']['duration'])])
    df1 = pd.DataFrame(IdDuration,columns = ['VideoID','Duration'])

    VideoStats = []
    for video in videos:
        title = video['snippet']['title']
        d = dateutil.parser.parse(video['snippet']['publishedAt'])
        publshedAt = d.strftime('%m/%d/%Y')
        temp= youtube.videos().list(id=video['snippet']['resourceId']['videoId'],part='statistics').execute()
        Vid = temp['items'][0]['id']
        viewCount = temp['items'][0]['statistics']['viewCount']
        likeCount = temp['items'][0]['statistics']['likeCount']
        dislikeCount = temp['items'][0]['statistics']['dislikeCount']
        commentCount = temp['items'][0]['statistics']['commentCount']
        VideoStats.append([Vid,title,publshedAt,viewCount,likeCount,dislikeCount,commentCount])
    
    df = pd.DataFrame(VideoStats, columns =['VideoID', 'Title', 'PublishedDate', 'ViewCount', 'LikeCount', 'DislikeCount', 'CommentCount'])
    df['Duration'] = df1['Duration']
    df['LikeCount'] = df['LikeCount'].astype(str).astype(int)
    df['ViewCount'] = df['ViewCount'].astype(str).astype(int)
    df['DislikeCount'] = df['DislikeCount'].astype(str).astype(int)
    df['CommentCount'] = df['CommentCount'].astype(str).astype(int)
    df['PublishedDate']= df['PublishedDate'].astype('datetime64[ns]')

    p = []
    n = []
    t = []
    videoids = df['VideoID']
    
    for videoid in videoids:
        comments = get_video_comments(part="snippet",videoId=videoid)
        pnt = findpnt(comments)
        p.append(pnt[0])
        n.append(pnt[1])
        t.append(pnt[2])
    df['P'] = p
    df['N'] = n
    df['T'] = t

    titles = df['Title'].tolist()
    for i in range(0,len(titles)):
        titles[i] = splitandlowercase(titles[i])

    cat = []
    for title in titles:
        count1 = cat1(title)
        count2 = cat2(title)
        count3 = cat3(title)
        count4 = cat4(title)
        countlist = [count1,count2,count3,count4]
        maxc = max(countlist)
        numberofoccur = countlist.count(maxc)
        if (count1 == count2 == count3 == count4 == 0) or numberofoccur > 1:
            cat.append(5)
        else:
            cat.append(countlist.index(maxc) + 1)

    df['Category'] = cat

    df.to_csv('C:/Users/Pritesh Gujarati/Desktop/Untitled Folder/yudi.csv',index=False)
    print("out mainfetch")
    return "done"


def get_channel_videos(channel_id):
    print("get_channel_videos")
    res = youtube.channels().list(id=channel_id,part='contentDetails').execute()
    playlist_id = res['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    
    videos = []
    next_page_token = None
    
    while 1:
        res = youtube.playlistItems().list(playlistId = playlist_id,part = 'snippet',maxResults=50,pageToken = next_page_token).execute()
        videos += res['items']
        next_page_token = res.get('nextPageToken')
        if next_page_token is None:
            break
    return videos

def get_video_stats(video_ids):
    print("get video stats")
    stats = []
    for i in range(0,len(video_ids),50):
        res = youtube.videos().list(id=','.join(video_ids[i:i+50]),part='statistics').execute()
        stats += res['items']
    return stats

def get_video_duration(video_ids):
    print("get duration")
    durations = []
    for i in range(0,len(video_ids),50):
        res = youtube.videos().list(id=','.join(video_ids[i:i+50]),part='contentDetails').execute()
        durations += res['items']
    return durations

def converttosec(duration):
    print("convert duration")
    dur=isodate.parse_duration(duration)
    return dur.total_seconds()

def splitandlowercase(title):
    print("split and lower")
    title =  title.lower()
    title = title.split(' ')
    return title 

def cat1(title):
    count = 0
    for everyword in title:
        if everyword in c1:
            count = count + 1
    return count

def cat2(title):
    count = 0
    for everyword in title:
        if everyword in c2:
            count = count + 1
    return count

def cat3(title):
    count = 0
    for everyword in title:
        if everyword in c3:
            count = count + 1
    return count

def cat4(title):
    count = 0
    for everyword in title:
        if everyword in c4:
            count = count + 1
    return count
        
def get_video_comments(**kwargs):
    print("get video comments")
    comments = []
    results = youtube.commentThreads().list(**kwargs).execute()
 
    while results:
        for item in results['items']:
            comment = item['snippet']['topLevelComment']['snippet']['textDisplay']
            comments.append(comment)
 
        if 'nextPageToken' in results:
            kwargs['pageToken'] = results['nextPageToken']
            results = youtube.commentThreads().list(**kwargs).execute()
        else:
            break
 
    return comments

def findpnt(comments):
    print("find pnt")
    p = 0
    n = 0
    t = 0
    for comment in comments:
        sent = TextBlob(comment).sentiment
        if (sent.polarity >0.0):
            p = p+1
        if (sent.polarity < 0.0):
            n = n+1
        else:
            t = t+1
    return [p,n,t]

def getAllInfo(g):
    print("get all info")
    UnivariateAnalysis = []
    for i in range(1,6):
        group = g.get_group(i)
        
        #get max and min views 
        a1 = group['ViewCount'].max()
        a2=group['ViewCount'].min()
        a3 = group['ViewCount'].mean()
        a4= group['LikeCount'].max()
        a5=group['LikeCount'].min()
        a6=group['LikeCount'].mean()
        a7 =group['DislikeCount'].max()
        a8=group['DislikeCount'].min()
        a9=group['DislikeCount'].mean()
        a10=group['CommentCount'].max()
        a11=group['CommentCount'].min()
        a12=group['CommentCount'].mean()
        if i== 1 :
            c =  'Admission Related'
        if i ==2 :
            c= 'Life Related'
        if i == 3:
            c = 'Q&A'
        else:
            c='Jobs and Career'
        UnivariateAnalysis.append([c,a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,a12])
    dfUA = pd.DataFrame(UnivariateAnalysis, columns = ['Category','MaxViews','MinViews','MeanViews','MaxLikes','MinLikes','MeanLikes' ,'MaxDislikes','MinDislikes','MeanDislikes','MaxComments','MinComments','MeanComments']) 
    dfUA.to_csv('/content/drive/My Drive/Colab Notebooks/UA.csv',index=False)
 
