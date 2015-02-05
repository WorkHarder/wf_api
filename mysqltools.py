#!/usr/bin/env python
#encoding:utf-8

''' Mysql python tools  by Sheng.Du @2015.1.8  Version: 1.0
    
    Class TableOp functions:
        insert,delete,search,update,count of table operations.
    Also, SQL can be execute by call TableOp._execute .
'''

import MySQLdb

class TableOp():
    ''' table operations class
    '''
    def __init__(self,host, user, passwd, db):
        try:
            self.conn,self.cur = self.connect(host, user, passwd, db)
        except:
            raise
    
    def insert(self,table_name,*doc_or_docs,**param):
        ''' insert a docment(s) into table
            params:
                table_name: table name
                doc_or_docs: a doc dict or a list(tuple) of doc dict
                param: extra condition such as--
                    cols: dest column names, default depends on doc keys
                    max_size: max number of doc in one SQL sentence
                return: number of affected rows.
        '''
        if isinstance(doc_or_docs[0], (list,tuple)):
            doc_or_docs = doc_or_docs[0]

        if "max_size" in param.keys():
            max_size = param["max_size"]
        else:
            max_size = 200
        
        if "cols" in param.keys():
            cols = param["cols"]
        else:
            cols = doc_or_docs[0].keys()
        
        datas = []

        for doc in doc_or_docs:
            v_list = [MySQLdb.escape_string(str(doc[col])) for col in cols]
            datas.append("("+str(v_list)[1:-1]+")")

        while len(datas) > max_size:
            SQL = "INSERT IGNORE INTO `%(table)s` (%(col)s) VALUES %(data)s"%\
                {"table":table_name,"col":','.join(cols),"data":','.join(datas[:max_size])}
            del datas[:max_size]
            self.cur.execute(SQL)

        SQL = "INSERT IGNORE INTO `%(table)s` (%(col)s) VALUES %(data)s"%\
            {"table":table_name,"col":','.join(cols),"data":','.join(datas[:max_size])}
        return self.cur.execute(SQL)
    
    def delete(self,table_name, conditions):
        ''' Delete a docment(s) from the table
            params: 
                table_name: dest table
                conditions: conditions to specilize the docment(s), conditions
                            can be a str/list/tuple
                        e.g.: "name='ds'" , ["name='ds'","age=23"]
            return: number of affected rows
        '''
        if isinstance(conditions, (tuple,list)):
            conds = [str(c) for c in conditions]
            conditions = " and ".join(conds)
        
        SQL = "DELETE FROM `%(table)s` WHERE %(cond)s"%\
            {"table":table_name,"cond":conditions}
        
        try:
            return self.cur.execute(SQL)
        except:
            raise
        #return self.cur.execute(SQL)
    
    def search(self,table_name,conditions,**param):
        ''' Search a docment(s) from table
            params:
                table_name: dest table name
                conditions: conditions to specilize the docment(s), conditions
                            can be a str/list/tuple
                param: extra condition such as--
                    cols: dest column names, default "*"
                    limit: a limit of result count, can be a int/list/tuple,
                        e.g.: limit=10, limit = (10,10)##(offset,count)
                return: num of affecting rows, dicts
        '''
        if isinstance(conditions, (tuple,list)):
            conds = [str(c) for c in conditions]
            conditions = " and ".join(conds)

        if "cols" in param:
            cols = ",".join(param["cols"])
        else:
            cols = "*"

        extra_str = ""
        if "limit" in param:
            if isinstance(param["limit"], (list,tuple)):
                extra_str = "limit "+",".join([str(i) for i in param["limit"]])
            else:
                extra_str = "limit "+str(param["limit"])
                
        SQL = "SELECT %(col)s FROM `%(table)s` WHERE %(cond)s"%\
            {"table":table_name,"col":cols,"cond":conditions} + extra_str
        #print SQL
        n = self.cur.execute(SQL)
        result = self.cur.fetchall()
        return n,(dict(zip(param["cols"],res)) for res in result)
    
    def update(self,table_name,conditions,changes):
        ''' Update a certain docment of this table
            params:
                table_name: dest table name
                conditions: conditions to specilize the docment(s), conditions
                            can be a str/list/tuple
                changes: update method, a list like ["col=v"]
            return: number of affecting rows
        '''
        if isinstance(conditions, (tuple,list)):
            conds = [str(c) for c in conditions]
            conditions = " and ".join(conds)

        sets = ",".join(changes)
        SQL = "UPDATE IGNORE `%(table)s` SET %(set)s WHERE %(conds)s"%\
            {"table":table_name,"set":sets,"conds":conditions}
        #print SQL
        n = self.cur.execute(SQL)
        return n
    
    def count(self,table_name,conditions=None):
        ''' Get the number of special docs
            param:
                table_name: dest table name
                conditions: conditions to specilize the docment(s),default is
                            None to count all docments
                return: number of the docments
        '''
        if isinstance(conditions, (tuple,list)):
            conds = [str(c) for c in conditions]
            conditions = " and ".join(conds)
        if conditions==None:
            SQL = "SELECT COUNT(*) FROM `%(table)s`"%{"table":table_name}
        else:
            SQL = "SELECT COUNT(*) FROM `%(table)s` WHERE %(conds)s"%\
                {"table":table_name,"conds":conditions}
        self.cur.execute(SQL)
        c = self.cur.fetchone()
        return int(c[0])

    def truncate(self,table_name):
        ''' Clear the dest table
        '''
        SQL = "TRUNCATE %(table)s"%{"table":table_name}
        return self.cur.execute(SQL)
    
    def _execute(self,sql):
        ''' execute input sqls
        '''
        n = self.cur.execute(sql)
        #print n
        return n,self.cur.fetchmany(n)

    def callproc(self,proc_name,*params):
        ''' Call a stored procedure.
        '''
        self.cur.callproc(proc_name,params)
        result = self.cur.fetchall()
        self.cur.close()
        self.cur = self.conn.cursor()
        return result

    def connect(self,host, user, passwd, db):
        conn = MySQLdb.connect(host=host, user=user, passwd=passwd, db=db)
        cur = conn.cursor()
        return conn,cur

if __name__ == "__main__":
    to = TableOp("localhost","root","lexxe","kbase_enwiki")
    #print to.insert("test",[{"id":12,"name":"dusheng"},{"name":"guoxiao","id":32}])
    print list(to.search("orgnization",["word=' The New Yorkz Times '","date='2013-12-09'"],cols=["times"])[1])
    #to.update("test",("name='dusheng'",),("name='ds'","id=17"))
    #print to.delete("test", ("name='guoxiao'"))
    #print to.count("test","name='ds'")
    #print to.search("test", "name='ds'")
    #print to._execute("update test set name='chenguanxi' where name='cgx'")
    #print to.callproc("id_name",1043783151)