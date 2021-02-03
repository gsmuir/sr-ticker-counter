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

### MAIN ######################################################################

# praw
reddit = praw.Reddit(
    user_agent="Comment Extraction", 
    client_id=prv_client_id,
    client_secret=prv_client_secret,
    username=prv_username,
    password=prv_password)

# params
subreddit_name = 'stocks'
comment_limit = 1000
upvotes = 0

# write counts out to h5 file
out_file = "out/out_tickers_" + subreddit_name + ".h5"

# time params 
s_datetime = datetime(2020, 10, 18, 0, 0, 0) # start date
e_datetime = datetime(2020, 11, 4, 0, 0, 0) # end date
time_step = timedelta(days=0, hours = 1, minutes = 0) # time block size

# has to be a better way than this rubbish... 
r_datetimes, r_datetimes_f, run_dates, run_datetimes_f, run_datetimes = {}, {}, [], [], []
t_datetime = s_datetime
while t_datetime < e_datetime:
    date_id = t_datetime.date().strftime("%b_%d_%Y")
    run_dates.append(date_id)
    run_datetimes_f.append(t_datetime.strftime("%b_%d_%Y_%H_%M"))
    run_datetimes.append(t_datetime)
    t_datetime = t_datetime + time_step

# split into lists by date
r_datetimes_f, r_datetimes = {}, {}
for date_i in set(run_dates):
    for date_idx in range(0,len(run_dates)):
        if date_i == run_dates[date_idx]:
            if date_i not in r_datetimes:
                r_datetimes[date_i], r_datetimes_f[date_i] = [], []
                r_datetimes_f[date_i].append(run_datetimes_f[date_idx])
                r_datetimes[date_i].append(run_datetimes[date_idx])
            else:
                r_datetimes_f[date_i].append(run_datetimes_f[date_idx])
                r_datetimes[date_i].append(run_datetimes[date_idx])

# go through dates and get the submissions, comments then count tickers
all_tickers = {}

sorteddates = sorted(r_datetimes.items(), key=lambda x: x[1])
for i, (date_id, day_dates) in enumerate(sorteddates):
    print("\nCounting subreddit: " + subreddit_name + " for day " + date_id, "(" + str(i+1) + " of", str(len(r_datetimes.items())) + ")" )
    day_tickers = []
    time_point_1 = int(day_dates[0].timestamp())
    while i < len(sorteddates) and time_point_1 < sorteddates[i+1][1][0].timestamp():

        # url for time points
        sid_url = "https://api.pushshift.io/reddit/search/submission/?subreddit=" + subreddit_name + "&sort=asc&sort_type=created_utc&after=" + str(time_point_1) + "&fields=id,created_utc,num_comments&size=1000"# + "&before="+str(time_point_2) + "&size=1000"
        print('\n\nGetting submission IDs from ', datetime.fromtimestamp(time_point_1))#, ' - ', datetime.fromtimestamp(time_point_2))

        # get request
        r = requests.get(sid_url)
        try:
            data = json.loads(r.text)
            tp_num_comments, tp_tickers, sub_time = 0, {},  []

            num_sub_beween_tp = len(data['data'])
            # time.sleep(1)
            print('Processing submissions and comments...')
            for sub in data['data']:

                if sub['num_comments'] == 0:
                    # skip submissions without comments.
                    continue

                # FIXME
                # if sub.created_utc is next day:
                #    break

                # get submission from praw
                sid = sub['id']
                time_point_1 = int(sub['created_utc']) + 1

                submission = reddit.submission(id=sid)
                sub_time = submission.created_utc
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
        except:
            print('Skipping.....')
            continue

        print("Found {s} submissions with {c} comments ".format( s=num_sub_beween_tp, c = tp_num_comments))
        res = dict(sorted(tp_tickers.items(), key=lambda item: item[1], reverse = True))
        print(res)
        day_tickers.append(res)
    
    # merge all over the day and then write the day
    all_tickers[date_id] = merge_timepoints(day_tickers)
    try:
        write_day(date_id = date_id, ticker_names = [*all_tickers[date_id].keys()], count_vals = [*all_tickers[date_id].values()], out_file = out_file)
    except:
        print("Failed during write out for " + date_id)
print("Done.")
