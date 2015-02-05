#!/usr/bin/python
#encoding:utf-8

import MySQLdb
from mysqltools import TableOp

buff = {}
COUNT = 0
MAX_LENGTH = 2000


word_clses = ["people","admin","org","event"]

def parser(data):
    global buff,COUNT,MAX_LENGTH
    date = data["date"].split(" ")[0]
    for cls in word_clses:
        if cls in data.keys():
            if cls not in buff.keys():
                buff[cls] = {}
            if date not in buff[cls].keys():
                buff[cls][date] = []
            buff[cls][date] += [MySQLdb.escape_string(i.strip()) for i in data[cls]]
            COUNT += len(data[cls])
            if COUNT >= MAX_LENGTH:
                flush()
                buff = {}
                
def flush():
    to = TableOp("localhost","root","lexxe","kbase_enwiki")
    for cls in buff.keys():
        insert_list = []
        for date in buff[cls].keys():
            w_list = buff[cls][date]
            w_set = set(w_list)
            for w in w_set:
                res = list(to.search(cls.lower(),("date='%s'"%date,\
                        "word=\"%s\""%MySQLdb.escape_string(w)),cols=["times"])[1])
                if res:
                    times = res.pop()["times"]
                    to.update(cls.lower(), ("date='%s'"%date,"word='%s'"%MySQLdb.escape_string(w)),
                               ["times=%s"%(times+w_list.count(w))])
                else:
                    #print w
                    insert_list.append(str((MySQLdb.escape_string(w),date.encode('utf-8'),w_list.count(w))))
        if insert_list:
            #print insert_list
            values = ",".join(insert_list)
            sql = 'insert into %s (word,date,times) values %s'
            #print sql%(cls.lower(),values)
            try:
                to._execute(sql%(cls.lower(),values))
            except:
                continue
      
if __name__ == "__main__":
    with open("/home/lexxe/myfile/myworkfile/projects_for_mysql/new_index_usa","r") as f:
        for data in f:
            parser(eval(data))
        flush()
    
    '''
    import os
    for f in os.listdir("/home/lexxe/myfile/myworkfile/projects_for_mysql/new/"):
        with open("/home/lexxe/myfile/myworkfile/projects_for_mysql/new/"+f) as g:
            for data in g:
                parser(eval(data))
            flush()
    '''
        
            