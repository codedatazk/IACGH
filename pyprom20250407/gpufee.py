#!/usr/bin/env python
# encoding: utf-8
import os
import datetime
import dboperate
import time
import sys
import re
import subprocess
from prometheus_client.core import GaugeMetricFamily, REGISTRY
from prometheus_client import make_wsgi_app
from wsgiref.simple_server import make_server

DBIP = '192.168.*.*'
DBUSER = 'username'
DBPASS = 'password'
DBNAME = 'route'
DBPORT = '3306'

class CustomCollector(object):
    def add(self, params):
        sum = 0
        for i in params:
            sum += int(i)
        return sum

    def collect(self):
        totgpucmd = subprocess.Popen("sinfo -h -o \"%n %G\"", stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell=True)
        outtotgpucmd = totgpucmd.communicate()[0]
        outtotgpucmd = outtotgpucmd.decode('utf-8')

        allocgpucmd = subprocess.Popen("sacct -a -X --format=Allocgres --state=RUNNING --noheader --parsable2", stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell=True)
        outallocgpucmd = allocgpucmd.communicate()[0]
        outallocgpucmd = outallocgpucmd.decode('utf-8')
        if outallocgpucmd:
            countalloc = re.findall(r'gpu:(\d+)', outallocgpucmd)
            totalalloc = self.add(countalloc)

        jobid = ""
        username = ""
        jobdurt = ""
        jobrunt = ""
        utcmd = subprocess.Popen("sacct -a -X --state=RUNNING --noheader --format=jobid,Allocgres --parsable2", stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell=True )
        oututcmd = utcmd.communicate()[0]
        oututcmd = oututcmd.decode('utf-8')
        if oututcmd:
            print(__file__, sys._getframe().f_lineno, oututcmd)
        for line in oututcmd.splitlines():
            startpos = line.find('gpu')
            jobid = line[0:(startpos-1)]
            cstcmd = subprocess.Popen("scontrol show job " + jobid + "|grep StartTime", stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell=True)
            outcstcmd = cstcmd.communicate()[0]
            outcstcmd = outcstcmd.decode('utf-8')

            soutcmd = subprocess.Popen("scontrol show job " + jobid + "|grep StdOut", stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell=True)
            outsoutcmd = soutcmd.communicate()[0]
            outsoutcmd = outsoutcmd.decode('utf-8')
            if outcstcmd:
                starttpos = outcstcmd.find('StartTime')
                endtpos = outcstcmd.find('EndTime')
                starttime = outcstcmd[(starttpos+10):(endtpos-1)]
                years = starttime[0:4]
                months = starttime[5:7]
                days = starttime[8:10]
                hours = starttime[11:13]
                minutes = starttime[14:16]
                seconds = starttime[17:19]
                starttime2 = datetime.datetime((int)(years), (int)(months), (int)(days), (int)(hours),(int)(minutes), (int)(seconds))

                stdoutpos = outsoutcmd.find('StdOut')
                fileout = outsoutcmd[(stdoutpos+7):(-1)]
                modifytime = time.localtime(os.stat(fileout).st_mtime)
                yearm =time.strftime('%Y', modifytime) 
                monthm = time.strftime('%m', modifytime)
                daym = time.strftime('%d', modifytime)
                hourm = time.strftime('%H', modifytime)
                minutem = time.strftime('%M', modifytime)
                secondm = time.strftime('%S', modifytime)
                modifytime2 = datetime.datetime((int)(yearm),(int)(monthm), (int)(daym), (int)(hourm), (int)(minutem),(int)(secondm))

                jobtimesec = (modifytime2 - starttime2).seconds
                jobtime = (int)(jobtimesec/60)
                jobrunt = jobtime


            squsercmd = subprocess.Popen("squeue --job " + jobid + "-a -r -h -o %u", stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell=True)
            outsqusercmd = squsercmd.communicate()[0]
            outsqusercmd = outsqusercmd.decode('utf-8')
            if outsqusercmd:
                username = outsqusercmd

            sqdtcmd= subprocess.Popen("squeue --job " + jobid + "-a -r -h -o %M", stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell=True)
            outsqdtcmd = sqdtcmd.communicate()[0]
            outsqdtcmd = outsqdtcmd.decode('utf-8')
            if outsqdtcmd:
                jobdurt = outsqdtcmd

            db = dboperate.connectdb(DBIP, DBUSER, DBPASS, DBNAME)
            db.set_character_set('utf8')
            cursor = db.cursor()
            chdbcmd = 'use ' + DBNAME
            retch = cursor.execute(chdbcmd)
            writetime = datetime.datetime.now().strftime("%Y-%m-%d-%H:%M:%S")
            wrmonth = datetime.datetime.now().strftime("%Y-%m-%d")
            selcmd = "select * from monthfee where task = '%d'" % (int)(jobid)
            cursor.execute(selcmd)
            selres = cursor.fetchone()
            db.commit
            if(selres == None):
                tup = (wrmonth, username, jobid, jobdurt, writetime, jobrunt)
                wrcmd = "insert into monthfee(fmonth, user, task, duralt, countt, jobrunt) values %s;"
                wrcmd2 = wrcmd % str(tup)
                cursor.execute(wrcmd2)
                db.commit()
            else:
                tup = (jobdurt)
                udjobdcmd = "update monthfee set duralt = '%s' where task = '%d'" %(jobdurt, (int)(jobid))
                udwtcmd = "update monthfee set countt = '%s' where task = '%d'" %(writetime, (int)(jobid))
                udjobrcmd = "update monthfee set jobrunt = '%s' where task = '%d'" %(jobrunt, (int)(jobid))
                cursor.execute(udjobdcmd)
                cursor.execute(udwtcmd)
                cursor.execute(udjobrcmd)
                db.commit()


        if outtotgpucmd:
            count = re.findall(r'gpu:(\d+)', outtotgpucmd)
            totalgpu = self.add(count)
            yield GaugeMetricFamily('slurm_gpu_total', 'totalCount', value=totalgpu)


REGISTRY.register(CustomCollector())

if __name__ == '__main__':
    coll = CustomCollector()

    app = make_wsgi_app()
    httpd = make_server('', 8000, app)
    httpd.serve_forever()
