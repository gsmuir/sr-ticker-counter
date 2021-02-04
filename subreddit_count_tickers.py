### AUTHORS ###################################################################
# Name      Date
# Gareth    Thu Jan  7 15:27:59 2021
#

### LIBRARIES #################################################################
import praw
import time
from datetime import datetime, timedelta
from words import *
from user import *
import json
import requests
import h5py
import itertools

### FUNCTIONS #################################################################
def merge_timepoints(dc):
    result = {} 
    for d in dc: 
        for k in d:
            result[k] = result.get(k, 0) + d[k]
    return(result)

def write_day(date_id, ticker_names, count_vals, out_file):
    print("\n Writing out " + date_id)
    out = h5py.File(out_file, 'a')
    try:    
        date_grp = out.create_group(date_id)
    except:
        print("Group already exists overwriting...\n")
        del out[date_id]
        date_grp = out.create_group(date_id)
    date_grp.create_dataset(name='Tickers', data=ticker_names)
    date_grp.create_dataset(name='Count', data=count_vals)
    out.close()

def get_pushshift_url(subreddit_name, time_point):
    # time point is unix UTC
    sid_url = ("https://api.pushshift.io/reddit/search/submission/?subreddit=" +
    subreddit_name + "&sort=asc&sort_type=created_utc&after=" +
    str(time_point) +
    "&fields=id,created_utc,num_comments&size=1000")# + "&before="+str(time_point_2) + "&size=1000"
    return(sid_url)

def is_next_day(tp1,tp2):
    return(datetime.fromtimestamp(tp2).date() > datetime.fromtimestamp(tp1).date())

def is_current_day(tp1,tp2):
    return(datetime.fromtimestamp(tp2).date() == datetime.fromtimestamp(tp1).date())

def to_date_str(ts):
   return(datetime.fromtimestamp(ts).strftime("%b_%d_%Y"))

def to_date(ts):
    return(datetime.date(ts))

def to_time_srt(ts):
    return(datetime.fromtimestamp(current_time).strftime(("%b-%d-%Y %H:%M")))
    

#import pdb; pdb.set_trace()

### MAIN ######################################################################

# praw
reddit = praw.Reddit(
    user_agent="Comment Extraction", 
    client_id=prv_client_id,
    client_secret=prv_client_secret,
    username=prv_username,
    password=prv_password)

# params
subreddit_name = 'wallstreetbets'
comment_limit = 1000
upvotes = 0

# write counts out to h5 file
out_file = "out/out_tickers_" + subreddit_name + ".h5"

# time params 
s_datetime = datetime(2020, 10, 17, 0, 0, 0) # start date
e_datetime = datetime(2020, 10, 20, 0, 0, 0) # end date
num_days = (to_date(e_datetime) - to_date(s_datetime)).days

# go through dates and get the submissions, comments then count tickers
all_tickers = {}

# current time is the 'After' time 
current_time = int(s_datetime.timestamp())

# initialise the ticker counter for this time point
tp_num_comments = 0
day_counter = 0
tp_tickers = {} 
day_start_time = time.time()
    
while current_time < int(e_datetime.timestamp()):
    
    print("\nCounting subreddit: " + subreddit_name + " for day " + to_time_srt(current_time) +  " (" + str(day_counter+1) + " of", str(num_days) + ")" )

    # url for time points
    sid_url = get_pushshift_url(subreddit_name, current_time)
    print('\n\nGetting submission IDs from', to_date_str(current_time)) #, ' - ', datetime.fromtimestamp(time_point_2))

    # get request
    r = requests.get(sid_url)

    try:
        data = json.loads(r.text)
        num_sub_beween_tp = len(data['data'])
        print('Processing submissions and comments...')
        for sub in data['data']:
            if sub['num_comments'] == 0:
                # skip submissions without comments.
                continue
            
            if is_current_day(current_time, sub['created_utc']):
                # get submission from praw
                sid = sub['id']
                current_time = int(sub['created_utc']) + 1
                #print(current_time,to_date_str(current_time))
                
                submission = reddit.submission(id=sid)
                submission.comment_sort = 'new'
                # skip submission if there are a massive amount of comments
                if submission.num_comments > 0 and submission.num_comments < 15000 :
                    submission.comments.replace_more(limit=comment_limit)
                    comments = submission.comments
                    for comment in comments:
                        tp_num_comments += 1
                        split = comment.body.split(" ")
                        for word in split:
                            word = word.replace("$", "")        
                            # word is uppercase, less than 5 characters, not in blacklist
                            if word.isupper() and len(word) <= 5 and word not in blacklist and word in stock_tickers:
                                if word in tp_tickers:
                                    tp_tickers[word] += 1
                                else:
                                    tp_tickers[word] = 1
                                    
            else:
                #print("HERE - this is the next day DAYDAY")
                print("Found {s} submissions with {c} comments ".format(s=num_sub_beween_tp, c = tp_num_comments))
                print("Time taken: " + str(round(int(time.time() - day_start_time) / 60)) + " minutes")
                time.sleep(5)
                print(to_date_str(current_time),to_date_str(sub['created_utc']))
                all_tickers[to_date_str(current_time)] = tp_tickers # merge_timepoints(tp_tickers)
                day_counter+=1
                
                # write out day
                try:
                    write_day(date_id = to_date_str(current_time), ticker_names = [*tp_tickers.keys()], count_vals = [*tp_tickers.values()], out_file = out_file)
                except:
                    print("Failed during write out for " + date_id)
                
                # reset ticker counter for next day
                current_time = int(sub['created_utc']) + 1
                tp_num_comments, tp_tickers = 0, {}

    except:
        print('Skipping.....')
        continue

print("Done.")
