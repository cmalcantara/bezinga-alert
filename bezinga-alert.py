#Dec 15, 2021
#A program that alerts the user when there are changes 
# in top gappers or premarket movers using bezinga data
# by Carlos Alcantara @cmalcantara / @cmaxyz

import pandas as pd
import winsound
import requests
import sys
import datetime
import sched, time
from bs4 import BeautifulSoup
from collections import OrderedDict as od
import pyttsx3


#STACKOVERFLOW MAGIC by Ji Wei (https://stackoverflow.com/questions/47131361/diff-between-two-dataframes-in-pandas)
def diff_func(df1, df2, uid, dedupe=True, labels=('df1', 'df2'), drop=[]):
    dict_df = {labels[0]: df1, labels[1]: df2}
    col1 = df1.columns.values.tolist()
    col2 = df2.columns.values.tolist()

    # There could be columns known to be different, hence allow user to pass this as a list to be dropped.
    if drop:
        print ('Ignoring columns {} in comparison.'.format(', '.join(drop)))
        col1 = list(filter(lambda x: x not in drop, col1))
        col2 = list(filter(lambda x: x not in drop, col2))
        df1 = df1[col1]
        df2 = df2[col2]


    # Step 1 - Check if no. of columns are the same:
    len_lr = len(col1), len(col2)
    assert len_lr[0]==len_lr[1], \
    'Cannot compare frames with different number of columns: {}.'.format(len_lr)

    # Step 2a - Check if the set of column headers are the same
    #           (order doesnt matter)
    assert set(col1)==set(col2), \
    'Left column headers are different from right column headers.' \
       +'\n   Left orphans: {}'.format(list(set(col1)-set(col2))) \
       +'\n   Right orphans: {}'.format(list(set(col2)-set(col1)))

    # Step 2b - Check if the column headers are in the same order
    if col1 != col2:
        print ('[Note] Reordering right Dataframe...')
        df2 = df2[col1]

    # Step 3 - Check datatype are the same [Order is important]
    if set((df1.dtypes == df2.dtypes).tolist()) - {True}:
        print ('dtypes are not the same.')
        df_dtypes = pd.DataFrame({labels[0]:df1.dtypes,labels[1]:df2.dtypes,'Diff':(df1.dtypes == df2.dtypes)})
        df_dtypes = df_dtypes[df_dtypes['Diff']==False][[labels[0],labels[1],'Diff']]
        print (df_dtypes)
    else:
        pass
        #print ('DataType check: Passed')

    # Step 4 - Check for duplicate rows
    if dedupe:
        for key, df in dict_df.items():
            if df.shape[0] != df.drop_duplicates().shape[0]:
                print(key + ': Duplicates exists, they will be dropped.')
                dict_df[key] = df.drop_duplicates()

    # Step 5 - Check for duplicate uids.
    if type(uid)==str or type(uid)==list:
        #print ('Uniqueness check: {}'.format(uid))
        for key, df in dict_df.items():
            count_uid = df.shape[0]
            count_uid_unique = df[uid].drop_duplicates().shape[0]
            var = [0,1][count_uid_unique == df.shape[0]] #<-- Round off to the nearest integer if it is 100%
            pct = round(100*count_uid_unique/df.shape[0], var)
            #print ('{}: {} out of {} are unique ({}%).'.format(key, count_uid_unique, count_uid, pct))

    # Checks complete, begin merge. '''Remenber to dedupe, provide labels for common_no_match'''
    dict_result = od()
    df_merge = pd.merge(df1, df2, on=col1, how='inner')
    if not df_merge.shape[0]:
        print ('Error: Merged DataFrame is empty.')
    else:
        dict_result[labels[0]] = df1
        dict_result[labels[1]] = df2
        dict_result['Merge'] = df_merge
        if type(uid)==str:
            uid = [uid]

        if type(uid)==list:
            df1_only = df1.append(df_merge).reset_index(drop=True)
            df1_only['Duplicated']=df1_only.duplicated(keep=False)  #keep=False, marks all duplicates as True
            df1_only = df1_only[df1_only['Duplicated']==False]
            df2_only = df2.append(df_merge).reset_index(drop=True)
            df2_only['Duplicated']=df2_only.duplicated(keep=False)
            df2_only = df2_only[df2_only['Duplicated']==False]

            label = labels[0]+' or '+labels[1]
            df_lc = df1_only.copy()
            df_lc[label] = labels[0]
            df_rc = df2_only.copy()
            df_rc[label] = labels[1]
            df_c = df_lc.append(df_rc).reset_index(drop=True)
            df_c['Duplicated'] = df_c.duplicated(subset=uid, keep=False)
            df_c1 = df_c[df_c['Duplicated']==True]
            df_c1 = df_c1.drop('Duplicated', axis=1)
            df_uc = df_c[df_c['Duplicated']==False]

            df_uc_left = df_uc[df_uc[label]==labels[0]]
            df_uc_right = df_uc[df_uc[label]==labels[1]]

            dict_result[labels[0]+'_only'] = df_uc_left.drop(['Duplicated', label], axis=1)
            dict_result[labels[1]+'_only'] = df_uc_right.drop(['Duplicated', label], axis=1)
            dict_result['Diff'] = df_c1.sort_values(uid).reset_index(drop=True)

    return dict_result


#Scheduler using sched by Nosklo https://stackoverflow.com/questions/474528/what-is-the-best-way-to-repeatedly-execute-a-function-every-x-seconds
s = sched.scheduler(time.time, time.sleep)
def search_premarket(sc):
    #print("Searching Bezinga Premarket Changes")

    global df1
    tables = pd.read_html(url)
    df2 = tables[table_index]

    difference = diff_func(df1, df2, uid)
    newStock = difference[uid].tolist()
    #print(difference['df2_only'])
    if bool(difference['df2_only'].empty):
        dtt = datetime.datetime.now()
        print(dtt + ' - ' + "None")
    else:
        winsound.Beep(freq, duration)
        #Testing the input data
        print("1m ago stocks: ")
        print(df1)
        print("\n")
        print("current stocks: ")
        print(df2)
        print("\n")

        #Text to Speech
        engine = pyttsx3.init()
        text = newStock
        engine.say(text)
        engine.runAndWait()

        #Timestamp and ouput
        dtt = datetime.datetime.now()
        print(dtt)
        print(difference['df2_only'])
        print("\n\n")

        #Write to file
        f = open(file, "a")
        for stock in newStock:
            tss = dtt + ' - ' + stock 
            f.writeline(tss)
        f.close()


    df1 = df2

    s.enter(waitTime, 1, search_premarket, (s,))

try:
    #Getting Data & Preprocessing
    waitTime = int(sys.argv[2])
    if sys.argv[1] == 'm':
        message = "Running Market Mover Search..."
        url = "https://www.benzinga.com/movers"
        table_index = 0
        uid = 'Symbol'
        file = 'market.txt'
    else:
        message = "Running Premarket Gapper Search..."
        url = "https://www.benzinga.com/premarket/"
        table_index = 5
        uid = 'Stock'
        file = 'premarket.txt'

    print(message)
    #Sound Settings
    duration = 800  # milliseconds
    freq = 440  # Hz

    tables = pd.read_html(url)
    df1 = tables[table_index]

    s.enter(waitTime, 1, search_premarket, (s,))
    s.run()
except IndexError:
    raise SystemExit(f"Usage: {sys.argv[0]} <'m' (market) or  'p'(premarket)> <sleep time>")

except KeyError:
    winsound.Beep(freq, duration)