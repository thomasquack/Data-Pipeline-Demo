import json
import praw
import boto3
from datetime import datetime

def lambda_handler(event, context):
    # init reddit client
    reddit = praw.Reddit(client_id = 'kCBo5gVihm2ulQ', 
              client_secret = 'wKOxCsnHFh14k5rkwdKyIhfW9mKWeQ', 
              user_agent = 'text summarizer')
    
    # get latest x posts
    subreddit = 'DataEngineering'
    new = reddit.subreddit(subreddit).new(limit = 500)
    new_data = []
    for post in new:
        post_data = [post.title, post.selftext]
        submission = reddit.submission(id = post.id)
        submission.comments.replace_more(limit = None)
        post_data.extend([comment.body for comment in submission.comments.list()])
        new_data.extend(post_data)
    
    # dump to s3
    s3 = boto3.resource('s3')
    date = datetime.now().strftime('%d%m%Y-%H%M')
    s3object = s3.Object('redditdata456346', f'reddit_data_{date}.json')
    s3object.put(
        Body = json.dumps(new_data).encode('UTF-8')
    )
            
    return {
        'statusCode': 200
    }
