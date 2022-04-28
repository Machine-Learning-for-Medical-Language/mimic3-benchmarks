import sys
import pandas as pd
from datetime import datetime
from os import walk
from os.path import join
from tqdm import tqdm
import json
#import seaborn as sns

def append_row_id(row, note):
    aa = note.loc[(note['HADM_ID'] == row['HADM_ID']) & (note['SUBJECT_ID'] == row['SUBJECT_ID'])]
    result = [i for i in aa['ROW_ID']]
    return result

#function removing rows without both charttime and storetime from "NOTEEVENTS.csv"
def keep_rowids_with_bothtimes(row, note):
    rows = row['row_ids']
    result = []
    for ids in rows:
        temp = note.loc[note['ROW_ID'] == ids]
        if temp['CHARTTIME'].any() != 'noval' and temp['STORETIME'].any() != 'noval':
            result.append(ids)
    return result

#functions removing rows without storetime from "NOTEEVENTS.csv"
def keep_rowids_with_storetimes(row, note):
    rows = row['row_ids']
    result = []
    for ids in rows:
        if note.loc[note['ROW_ID'] == ids]['STORETIME'].any() != 'noval':
            result.append(ids)
    return result

#func taking a row and get unique row_ids matches and elimate no charttime notes
def append_row_id_with_time(row, df):
    aa = df.loc[(df['HADM_ID'] == row['HADM_ID']) & (df['SUBJECT_ID'] == row['SUBJECT_ID']) & (df["CHARTTIME"].notnull())]
    result = [i for i in aa['ROW_ID']]
    return result

#func taking a row and get unique row_ids matches and elimate no charttime notes + radiology notes
def append_row_id_time_rad(row, df):
    aa = df.loc[(df['HADM_ID'] == row['HADM_ID']) & (df['SUBJECT_ID'] == row['SUBJECT_ID']) & (df["CATEGORY"] == "Radiology")]
    result = [i for i in aa['ROW_ID']]
    return result

def time_diff (intime, storetime):
    exout = datetime.strptime(storetime, "%Y-%m-%d %H:%M:%S")
    exin = datetime.strptime(intime, "%Y-%m-%d %H:%M:%S")
    difference = exout - exin 
    duration_in_s = difference.total_seconds() 
    return duration_in_s/86400

#notes with storetime to timemarks
def rowid_and_timemarks(row, note, adm_id_to_filename):
    rows = row['row_ids_storetimes']
    result = []
    for ids in rows:
        temp = {}
        filename = adm_id_to_filename[row['HADM_ID']]
#         if row['INTIME'] and note.iloc[ids]['STORETIME']:
        time = time_diff(str(row['INTIME']), str(note.loc[note['ROW_ID'] == ids]['STORETIME'].any()))
        temp[ids] = [filename, time]
        result.append(temp)
    return result

def write_row_ids(file2row, path, filename):
    ### file2row is the json dict we imported, path is your designate directory, filename is filename you need
    df = pd.read_csv(os.path.join(path, filename), encoding='utf-8')
    filling = file2row[filename]
    #print(filling)
    adm_id = filenames2admission_ids[filename]
    los = float(all_stay[all_stay['HADM_ID'] == int(adm_id)]['LOS'] * 24)
#     out_yet = []
    res = []
    temp = []
    counter = 0
    for index in range(len(df)):
        hour = df.iloc[index]['Hours']
#         if hour < los:
#             out_yet.append(0)
#         else: 
#             out_yet.append(1)
        while counter < len(filling):
            if hour >= filling[counter][1]: 
                temp.append(filling[counter][0])
                counter += 1
                #print(temp)
            else:
                break
        res.append(temp.copy())
    df['row_list'] = res
    #df['out_yet'] = out_yet
    df.to_csv(os.path.join(str(path),filename), index=False)
    return df

def main(args):
    if len(args) < 2:
        sys.stderr.write('Required arg(s): <NOTEEVENTS.csv path> <data root>\n')

    print("Reading in NOTEEVENTS.csv file")
    note = pd.read_csv(args[0])
    #deal with datas
    note.fillna("noval", inplace = True)
    
    print("Reading in all_stays.csv file")
    all_stay = pd.read_csv(join(args[1], 'all_stays.csv'))
    print("Appending row ids to each stay")    
    print("Step 1...")
    all_stay['row_ids'] = all_stay.apply(append_row_id, args=(note,), axis=1)
    print("Step 2...")
    all_stay['row_ids_both_times'] = all_stay.apply(keep_rowids_with_bothtimes, args=(note,), axis=1)
    print("Step 3...")
    all_stay['row_ids_storetimes'] = all_stay.apply(keep_rowids_with_storetimes, args=(note,), axis=1)
    print("Step 4...")
    all_stay['row_ids_radi'] = all_stay.apply(append_row_id_time_rad, args=(note,), axis=1)
    print("Step 5...")
    all_stay['row_ids_time'] = all_stay.apply(append_row_id_with_time, args=(note,), axis=1)
    
    print("Building mappings from admission to subject...")
    adm_to_subject_mapping = {id:[] for id in all_stay['SUBJECT_ID'].tolist()}
    print("Length of subject mapping is %d, expected 42276" % (len(adm_to_subject_mapping)))
#     assert len(adm_to_subject_mapping) == 42276, 'Subject ID list has the wrong length!'

    #we need a HADM_ID subject_id_episode look up dictionary
    for subject_id, hadm_id in zip(all_stay['SUBJECT_ID'], all_stay['HADM_ID']):
        adm_to_subject_mapping[int(subject_id)].append(int(hadm_id))

    #we need a HADM_ID filenames look up dictionary
    adm_id_to_filename = {}
    for key in adm_to_subject_mapping:
        for index,ids in enumerate(adm_to_subject_mapping[key]):
            #index = index+1
            filename = str(key)+"_episode"+str(index+1)+"_timeseries.csv"
            adm_id_to_filename[ids] = filename
    
    all_stay['timestamps'] = all_stay.apply(rowid_and_timemarks, args=(note,adm_id_to_filename), axis=1)
    timestamps = all_stay['timestamps']
    
    #pack up the dict with our values
    file2row_id = {adm_id_to_filename[k]: [] for k in adm_id_to_filename}
    for row in timestamps:
        temp = row
        for k in temp:
            for key in k:
                #print(key)
                filename = k[key][0]
                time = k[key][1]
                file2row_id[filename].append((key,time*24))

                
    #sorting the hours in chronological order
    for filename in file2row_id:
        file2row_id[filename].sort(key=lambda x: x[1])
    
    
    with open('file2row_id.json', 'w') as fp:
        json.dump(file2row_id, fp)    

    filenames2admission_ids = {value:key for key, value in adm_id_to_filename.items()}
    train_dir = join(args[1], 'train')
    test_dir = join(args[1], 'test')
    
    f = []
    for (dirpath, dirnames, filenames) in walk(train_dir):
        f.extend(filenames)
        break
    for (dirpath, dirnames, filenames) in walk(test_dir):
        f.extend(filenames)
        break
    
    train_list = []
    test_list = []

    for (dirpath, dirnames, filenames) in walk(train_dir):
        train_list.extend(filenames)
        break
    for (dirpath, dirnames, filenames) in walk(test_dir):
        test_list.extend(filenames)
        break

    file2rows = {}
    for filename in train_list:
        try:
            file2rows[filename] = write_row_ids(file2row_id, train_dir, filename)
        except:
            print("Sorry ! File not find ")    

    for filename in test_list:
        try:
            file2rows[filename] = write_row_ids(file2row_id, test_dir, filename)
        except:
            print("Sorry ! File not find ")


    #mapping from row_id to the real doc notes
    file2row_id_text = {}
    for filename in file2row_id:
        result = []
        for sets in file2row_id[filename]:
            temp = list(sets)
            temp.append(note.loc[note['ROW_ID'] == sets[0]]["TEXT"].any())
            result.append(temp)
        file2row_id_text[filename] = result
    
    # sort in time manner
    for filename in file2row_id:
        file2row_id_text[filename].sort(key=lambda x: x[1])

        
    filenames = list(file2row_id.keys())
    file2class = {}

    """
    0: los <= 1 ==> one for ICU stays shorter than a day

    seven day-long buckets for each day of the first week:

    1: 1 < los <= 2

    2: 2 < los <= 3

    3: 3 < los <= 4

    4: 4 < los <= 5

    5: 5 < los <= 6

    6: 6 < los <= 7

    7: 7 < los <= 8

    8: 8 < los <= 14 ==> one for stays of over one week but less than two

    9: los > 14 ==> one for stays of over two weeks
    """

    for i in tqdm(range(len(filenames))):
        filename = filenames[i]                
        adm_id = filenames2admission_ids[filename]
        los = float(all_stay[all_stay['HADM_ID'] == int(adm_id)]['LOS'])
        if los <= 1:
            file2class[filename] = 0
        elif los > 1 and los <= 2:
            file2class[filename] = 1
        elif los > 2 and los <= 3:
            file2class[filename] = 2
        elif los > 3 and los <= 4:
            file2class[filename] = 3
        elif los > 4 and los <= 5:
            file2class[filename] = 4
        elif los > 5 and los <= 6:
            file2class[filename] = 5
        elif los > 6 and los <= 7:
            file2class[filename] = 6
        elif los > 7 and los <= 8:
            file2class[filename] = 7
        elif los > 8 and los <= 14:
            file2class[filename] = 8
        else:
            file2class[filename] = 9
    
    #class_count = [count for _,count in file2class.items()]
    #sns.distplot(class_count)
    
    with open('adm_to_subject_mapping.json', 'w') as fp:
        json.dump(adm_to_subject_mapping, fp)

    with open('adm_id_to_filename.json', 'w') as fp:
        json.dump(adm_id_to_filename, fp)

    with open('file2rows_list.json', 'w') as fp:
        json.dump(file2rows, fp)  

    with open('file2row_id_text.json', 'w') as fp:
        json.dump(file2row_id_text, fp)

    with open("file2class.json", 'w') as fp:
        json.dump(file2class, fp)

    all_stay.to_csv("all_stay_timestamp.csv", encoding='utf-8', index=False)

if __name__ == '__main__':
    main(sys.argv[1:])
