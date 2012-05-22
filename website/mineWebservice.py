#!/usr/local/bin/python

from pymongo import Connection
import os
import logging
from datetime import datetime
import smtplib
import string
from ftplib import FTP

#connect to mine database
HOST = '127.0.0.1'
PORT = 27017
connection = Connection(HOST, PORT)
db = connection.MINE


def postRequest(studyid, email, processed = False, queued = False):
        if not alreadyRequested(studyid):
                request = {"gse": studyid, "email": email, "datetime": datetime.now(), "processed": False, "queued":False}
                db.request.insert(request)

def alreadyRequested(studyid):
        if db.request.find({"gse":studyid}).count() > 0:
                return True
        else:
                return False

def requestProcessed(studyid):
        if db.request.find({"gse":studyid, "processed":True}).count() > 0:
                return True
        else:
                return False

def requestQueued(studyid):
        if db.request.find({"gse":studyid, "queued":True}).count() > 0:
                return True
        else:
                return False

def markRequestProcessed(studyid):
        db.request.update({"gse":studyid,}, {"$set": {"processed":True,"queued":False}})
        email = db.request.find({"gse":studyid}).distinct("email")
        sendEmail(email, studyid)

def markRequestQueued(studyid):
        db.request.update({"gse":studyid,}, {"$set": {"queued":True}})

def getStudyList():
        return db.request.find().distinct("gse")

def getQueuedStudyList():
	return db.request.find({"queued":True}).distinct("gse")

def getProcessedStudyList():
	return db.request.find({"proccessed":True}).distinct("gse")

def isValidNumber(studyid):
    ftp = FTP('ftp.ncbi.nih.gov')
    ftp.login()
    ftp.cwd('/pub/geo/DATA/SeriesMatrix/')
    try:
        ftp.cwd(studyid)
    except:
        ftp.quit()
        return False

    ftp.quit()
    return True

def sendEmail(address, studyid):
    MESSAGE = 'Your request for study ' + studyid + ' has been processed \n\n to view go to http://yates.webfactional.com/studies/'+studyid
    SENDER = 'noreply@yates.webfactional.com'
    SUBJECT = 'Mine has processed your study!'

    Body = string.join(( "From: %s" % SENDER,
                         "To: %s" % address,
                         "Subject: %s" % SUBJECT,
                         "",
                         MESSAGE
                         ), "\r\n")
    
    server = smtplib.SMTP('smtp.webfaction.com')
    server.login('minebox','b0d79559')
    server.sendmail(SENDER, [address], Body)
    server.quit()

def remove(studyid, email):
        db.request.remove({"gse":studyid, "email":email})

def removeByNumber(studyid):
        db.request.remove({"gse":studyid})

def removeByEmail(email):
        db.request.remove({"email":email})

def uploadLine(studyid, varname, floats): 
        line = {"id": varname, "data": floats}
        db[studyid].insert(line)


def RetrieveData(studyid, id):
        return map(float, db[studyid].find({"id":id}).distinct("data"))

def uploadStudy(studyid):

        #logging setup
        logging.basicConfig(filename='Upload.log',level=logging.INFO)

        #path for study files
        path = 'OUT/'
        listing = os.listdir(path)

        #for all studies (exclude the file log.txt)
        for file in listing:
                if not file == 'log.txt':
                        try:
                                #open the file
                                logging.info("trying to open " + file)
                                f = open(path + file)
                                count = 0

                                #for each line after the header
				logging.info("trying to read lines")
                                for line in f:
                                        count = count + 1
                                        if count > 3:
                                                try:
                                                        #insert lines
                                                        cells = line.split("\t")
                                                        uploadLine(studyid, cells[0], cells[1:])

                                                except:
                                                        logging.error("line " + count + " could not be read")

                                #close file
                                logging.info("closing " + file)
                                f.close()

				#mark request queued
				markRequestQueued(studyid)
                        except:
                                logging.error("Could not open " + file)

                                

def uploadProcessedData(studyid, var1, var2, mic, pcc):
        data = {"var1": var1,"var2":var2, "mic": mic, "pcc": pcc}
        db[studyid].insert(data)

def uploadProcessedStudy(studyid, path, varfile, micfile, pccfile):
        logging.basicConfig(filename='DEBUG.log',level=logging.INFO)

        logging.info("opening " + varfile)
        try:
                vars = open(path+varfile)
		vars.close()
        except:
                logging.error("could not open " + varfile)
        
        logging.info("opening " + micfile)
        try:        
                mic = open(path+micfile)
		mic.close()
        except:
                logging.error("could not open " + micfile)
                
        logging.info("opening " + pccfile)
        try:
                pcc = open(path+pccfile)
		pcc.close()
        except:
                logging.error("could not open " + pccfile)
                
        count = 0

	vars = open(path + varfile)
	mic = open(path + micfile)
	pcc = open(path + pccfile)
        for line in vars:
                count += 1
		variables = []
		micvalue = ""
		pccvalue = ""

                try:
                        variables = line.split(',')
			variables[1] = variables[1].rstrip()
                except:
                        logging.error("could not read line " + count + " from " + varfile)
                try:
                        micvalue = mic.readline()
			micvalue = micvalue.rstrip()
                except:
                        logging.error("could not read line " + count + " from " + micfile)
                try:
                        pccvalue = pcc.readline()
			pccvalue = pccvalue.rstrip()
                except:
                        logging.error("could not read line " + count + " from " + micfile)

                uploadProcessedData(studyid+"P", variables[0], variables[1], micvalue, pccvalue)

        logging.info("done uploading lines")

        logging.info("closing files")

        vars.close()
        mic.close()
        pcc.close()
 
#return pcc value for two variables
def getPccData(studyid, var1, var2):

        x = db[studyid+"P"].find({"var1":var1, "var2":var2}).distinct("pcc")
	y = db[studyid+"P"].find({"var2":var1, "var1":var2}).distinct("pcc")
        if x.count > 0:
		return x
	else:
		return y

#return mic value for two variables
def getMicData(studyid, var1, var2):

        x = db[studyid+"P"].find({"var1":var1, "var2":var2}).distinct("mic")
        y = db[studyid+"P"].find({"var2":var1, "var1":var2}).distinct("mic")
        if x.count > 0:
                return x
        else:
                return y

def getCorrespondingVars(studyid, var):
	
	x = db[studyid+"P"].find({"var1":var}).distinct("var2") 
	y = db[studyid+"P"].find({"var2":var}).distinct("var1")
	return x+y
