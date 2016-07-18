
# # Write out to the sqlite database using scraperwiki library
# scraperwiki.sqlite.save(unique_keys=['name'], data={"name": "susan", "occupation": "software developer"})
#
# # An arbitrary query against the database
# scraperwiki.sql.select("* from data where 'name'='peter'")

# You don't have to do things with the ScraperWiki and lxml libraries.
# You can use whatever libraries you want: https://morph.io/documentation/python
# All that matters is that your final data is written to an SQLite database
# called "data.sqlite" in the current working directory which has at least a table
# called "data".
#import os
#os.system("pip install scraperwiki")
#os.system("pip install mechanize")
#os.system("pip install pandas")

import scraperwiki
import requests
import mechanize
from bs4 import BeautifulSoup
import pandas as pd
import json
import datetime
import time
from dateutil import parser

br = mechanize.Browser()
url='https://acp.planninginspectorate.gov.uk/CaseSearch.aspx'
br.open(url)

br.select_form(nr=0)

br.form['ctl00$cphMainContent$txtSearchLPA'] = 'Isle of Wight Council'
br.form['ctl00$cphMainContent$cboSearchLPA']=['P2114',]
req = br.submit(name='ctl00$cphMainContent$cmdSearch')

doc=req.read()
doc=doc.replace('\r','').replace('\n','')
doc=doc.replace('<br/>',', ')

df=pd.read_html(doc,header=0)[0]
df.rename(columns={'Appellant/Applicant':'AppellantApplicant','Case Reference':'CaseReference','Site Address':'SiteAddress','Case Type':'CaseType'}, inplace=True)

t='IWPLANNINGAPPLICATIONAPPEALS'

dt="CREATE TABLE IF NOT EXISTS 'IWPLANNINGAPPLICATIONAPPEALS' ('CaseReference' text,'SiteAddress' text,'AppellantApplicant' text,'Authority' text,'Case Type' text,'Status' text)"
scraperwiki.sqlite.execute(dt)
	
dfd=df.to_dict(orient='records')
newRecords=[]
updateRecords=[]
grabber=[]
cases=[x['CaseReference'] for x in scraperwiki.sqlite.select("CaseReference from {t}".format(t=t))]
inprogress=[x['CaseReference'] for x in scraperwiki.sqlite.select("CaseReference from {t} where Status='In Progress'".format(t=t))]
for r in dfd:
	#if len(scraperwiki.sqlite.select("* from {t} where CaseReference='{n}'".format(t=t,n=r['CaseReference'])))==0:
	if r['CaseReference'] not in cases:
		print('First seen: {}'.format(r['CaseReference']))
		r['firstSeen']=datetime.datetime.utcnow()
		newRecords.append(r)
		grabber.append(r['CaseReference'])
	elif (r['Status']=='Complete: Decision issued') and r['CaseReference'] in inprogress:
		updateRecords.append(r)
		grabber.append(r['CaseReference'])
#	elif (r['Status']=='Complete: Decision issued') and (len(scraperwiki.sqlite.select("* from {t} where CaseReference='{n}' and Status='In Progress'".format(t=t,n=r['CaseReference'])))==1):
#		updateRecords.append(r)
#		grabber.append(r['CaseReference'])
if len(newRecords)>0:
	print('Adding {} new records'.format(len(newRecords)))
	scraperwiki.sqlite.save(unique_keys=['CaseReference'],table_name=t, data=newRecords)
if len(updateRecords)>0:
	print('Updating {} records'.format(len(updateRecords)))
	scraperwiki.sqlite.save(unique_keys=['CaseReference'],table_name=t, data=updateRecords)

def dateSetter(ds):
	if ds=='' or ds=='N/A': return ''
	try:
		dst=parser.parse(ds, dayfirst=True)
		dst=datetime.date(dst.year,dst.month,dst.day)
	except:
		dst=''
	return dst
	
def appealScrape(caseRef):
    print('Getting details for {}'.format(caseRef))
    urlstub="https://acp.planninginspectorate.gov.uk/ViewCase.aspx?CaseID={}&CoID=0"
    cid=caseRef.split('/')[-1]
    url=urlstub.format(cid)

    response =requests.get(url)
    soup=BeautifulSoup(response.content)
    t=soup.find("table", { "class" : "repeater" })

    d={}
    rows= t.findAll('tr')
    for row in rows[1:-1]:
      cells=row.findAll('td')
      d[cells[0].text.strip()]=cells[1].text.strip()
      if len(cells)==4: d[cells[2].text.strip()]=cells[3].text.strip()
    
    #parse dates: "Statement(s) due","Questionnaire due","Event Date","Appellant/LPA Final Comments due","Inquiry Evidence due"
    for el in ['Start Date','Interested Party Comments due',"Decision Date"]:
    	d["{}_t".format(el.replace(' ',''))]=dateSetter( d[el] )

    d['linked']=rows[-1].find('td').text
    d['ref']=soup.find('h1',id='cphMainContent_LabelCaseReference').text.replace('Reference:','').strip()
    
    tmp=soup.find('span',id='cphMainContent_labName')
    tmp=tmp['title'] if 'title' in tmp else ''
    d['appellant']=tmp
    
    tmp=soup.find('span',id='cphMainContent_labAgentName')
    tmp=tmp['title'] if 'title' in tmp else ''
    d['agent']=tmp
    
    tmp=soup.find('span',id='cphMainContent_labSiteAddress')
    tmp=tmp['title'] if 'title' in tmp else ''
    d['address']=tmp
    
    l=[]
    if 'linked' in d:
        for lnk in d['linked'].split('\n'):
            ll={'caseRef':d['ref']}
            if lnk.startswith('Lead Case'):
                ll['k']=d['ref']+':'+lnk.split('-')[1].strip()
                ll['type']='Lead'
                ll["linkref"]=lnk.split('-')[1].strip()
                l.append(ll)
            elif lnk.startswith('Linked Case'):
                ll['k']=d['ref']+':'+lnk.split('-')[1].strip()
                ll['type']='Linked'
                ll["linkref"]=lnk.split('-')[1].strip()
                l.append(ll)
    time.sleep(1)
    return d,l


#zz=df['CaseReference'].head(2).apply(lambda x: appealScrape(x))
zz=df[df['CaseReference'].isin(grabber)]['CaseReference'].apply(lambda x: appealScrape(x))
caseDetails=[list(z) for z in zip(*zz)]

cc=[]
if len(caseDetails)>0:
    for c in caseDetails[1]:
        for ci in c:
            cc.append(ci)

    scraperwiki.sqlite.save(unique_keys=['ref'],table_name='casedetails', data=caseDetails[0])
    scraperwiki.sqlite.save(unique_keys=['k'],table_name='linkedcases', data=cc)
else: print("All up to date...")

