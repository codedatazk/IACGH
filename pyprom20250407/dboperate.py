#!/usr/bin/env python
# encoding: utf-8
import MySQLdb
import sys

def connectdb(dbip, dbuser, dbpass, dbname):
    try:
        db = MySQLdb.connect(dbip, dbuser, dbpass, dbname)
        db.set_character_set('utf8')
        if(db == None):
            return None
        return db
    except Exception as e:
        print (e)
        return None

