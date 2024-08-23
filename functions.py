from datetime import date, time,timedelta
import requests
import psycopg2
import pymssql
import os
import constants
import json
from zeep import Client
import base64

#---------------------- For Document Chain to Fetch all the Certificates----------------------------------------
def getDocumentType(sm_state_mnemonic, dm_department_mnemonic, certtype_mnemonic):
    try:
        sql_conn = pymssql.connect(constants.db_host, constants.db_user, constants.db_pwd, constants.db_cert)        
        cursor = sql_conn.cursor()
        sql = ("select SM.SM_State_Name as StateName, DM.DM_Dapartment_Name as DeptName, CM.CTM_CertType_Name as CertName from State_Master as SM join Department_Master as DM "
               " on DM.DM_State_Code = SM.SM_State_Code join Certificates_Master as CM on CM.CTM_State_Code = DM.DM_State_Code and CM.CTM_Department_Code = DM.DM_Department_Code  join User_Master as UM on DM.DM_State_Code=UM.Um_State_code and DM.DM_Department_Code = UM.Um_Department_code "
               " where SM.SM_State_Mnemonic = '{}' and DM.DM_Department_Mnemonic = '{}' and CM.CTM_CertType_Mnemonic = '{}' and UM.UM_ChainFlag='D' ").format(sm_state_mnemonic, dm_department_mnemonic, certtype_mnemonic)

        #print(sql)
        cursor.execute(sql)
        data = dictfetchall(cursor)
        if data == []:           
            return "NotExist"
        else:
            dbcertdata = data
            return dbcertdata  
    except Exception as e:
        print(e)
        return
    finally:
        sql_conn.close()    

#---------------------- For Judiciary Chain to Fetch all the Certificates----------------------------------------
def getJudiciaryType(sm_state_mnemonic, dm_department_mnemonic, certtype_mnemonic):
    try:
        sql_conn = pymssql.connect(constants.db_host, constants.db_user, constants.db_pwd, constants.db_cert)        
        cursor = sql_conn.cursor()
        sql = ("select SM.SM_State_Name as StateName, DM.DM_Dapartment_Name as DeptName, CM.CTM_CertType_Name as CertName from State_Master as SM join Department_Master as DM "
               " on DM.DM_State_Code = SM.SM_State_Code join Certificates_Master as CM on CM.CTM_State_Code = DM.DM_State_Code and CM.CTM_Department_Code = DM.DM_Department_Code  join User_Master as UM on DM.DM_State_Code=UM.Um_State_code and DM.DM_Department_Code = UM.Um_Department_code "
               " where SM.SM_State_Mnemonic = '{}' and DM.DM_Department_Mnemonic = '{}' and CM.CTM_CertType_Mnemonic = '{}' and UM.UM_ChainFlag='J' ").format(sm_state_mnemonic, dm_department_mnemonic, certtype_mnemonic)

        #print(sql)
        cursor.execute(sql)
        data = dictfetchall(cursor)
        if data == []:           
            return "NotExist"
        else:
            dbcertdata = data
            return dbcertdata  
    except Exception as e:
        print(e)
        return
    finally:
        sql_conn.close()
#---------------------- For Certificate Chain to Fetch all the Certificates----------------------------------------
def getCertificateType(sm_state_mnemonic, dm_department_mnemonic, certtype_mnemonic):
    try:
        sql_conn = pymssql.connect(constants.db_host, constants.db_user, constants.db_pwd, constants.db_cert)        
        cursor = sql_conn.cursor()
        sql = ("select SM.SM_State_Name as StateName, BU.BU_Board_Name as DeptName, CM.CTM_CertType_Name as CertName from State_Master as SM join Board_Univ_Master as BU "
               " on BU.BU_State_Code = SM.SM_State_Code join Certificates_Master as CM on CM.CTM_State_Code = BU.BU_State_Code and CM.CTM_Department_Code = BU.BU_Board_Code "
               " where SM.SM_State_Mnemonic = '{}' and BU.BU_Board_Mnemonic = '{}' and CM.CTM_CertType_Mnemonic = '{}'  ").format(sm_state_mnemonic, dm_department_mnemonic, certtype_mnemonic)
        #print(sql)
        cursor.execute(sql)
        data = dictfetchall(cursor)
        if data == []:           
            return "NotExist"
        else:
            dbcertdata = data
            return dbcertdata  
    except Exception as e:
        print(e)
        return
    finally:
        sql_conn.close()
#---------------------- For Property Chain to Fetch all the Certificates----------------------------------------
def getPropertyType(sm_state_mnemonic, dm_department_mnemonic, certtype_mnemonic):
    try:
        sql_conn = pymssql.connect(constants.db_host, constants.db_user, constants.db_pwd, constants.db_property)        
        cursor = sql_conn.cursor()
        sql = ("select SM.SM_State_Name as StateName, DM.DM_Dapartment_Name as DeptName, CM.CTM_CertType_Name as CertName from State_Master as SM join Department_Master as DM "
               " on DM.DM_State_Code = SM.SM_State_Code join Certificates_Master as CM on CM.CTM_State_Code = DM.DM_State_Code and CM.CTM_Department_Code = DM.DM_Department_Code "
               " where SM.SM_State_Mnemonic = '{}' and DM.DM_Department_Mnemonic = '{}' and CM.CTM_CertType_Mnemonic = '{}' ").format(sm_state_mnemonic, dm_department_mnemonic, certtype_mnemonic)
        #print(sql)
        cursor.execute(sql)
        data = dictfetchall(cursor)
        if data == []:           
            return "NotExist"
        else:
            dbcertdata = data
            return dbcertdata  
    except Exception as e:
        print(e)
        return
    finally:
        sql_conn.close()

# For state wise

def getState(chainurl):
    try:  
        URL=chainurl+'/getstate/'      
        headers = {"Content-Type": "application/json; charset=utf-8"}                 
        response = requests.post(URL, headers=headers).json()  
        
        return response
    except Exception as e:       
        print(e)
        print("error while calling API".format(chainurl))

# On board Users wrt state

def getOnboardedUsers(chainurl,state_mnemonic):
    try:  
        URL=chainurl+'/getonboardedusers/'  
        data ={
            "state_mnemonic": state_mnemonic
            }
        headers = {"Content-Type": "application/json; charset=utf-8"}                 
        response = requests.post(URL, json=data, headers=headers).json()       
        
        return response
    except Exception as e:       
        print(e)
        print("error while calling API".format(chainurl))  


# Get Error Logs based on the date:

def getErrorLogs(chainurl,count_date):
    try:  
        URL=chainurl+'/getbcerrorlogs/'
        print(URL)   
        data ={
            "count_date": count_date
            }
        print(data)
        headers = {"Content-Type": "application/json; charset=utf-8"}                 
        response = requests.post(URL, json=data, headers=headers).json()       
        
        return response
    except Exception as e:       
        print(e)
        print("error while calling API bcerr".format(chainurl))

# Verifier Reads:

def getReadsByVerifier(chainurl,state_mnemonic, department_mnemonic, certtype_mnemonic, count_date):
    
    try:  
        URL=chainurl+'/getreadsbyverifier/'
        
        data ={
            "state_mnemonic": state_mnemonic,
            "department_mnemonic": department_mnemonic,
            "certtype_mnemonic": certtype_mnemonic,
            "count_date": count_date
            }
        headers = {"Content-Type": "application/json; charset=utf-8"}                 
        response = requests.post(URL, json=data, headers=headers).json()       
        
        return response
    except Exception as e:       
        print(e)
        print("error while calling API".format(chainurl)) 

# Get Total Txn users

def getTxnCounterUsers(chainurl,username, count_date):
    print()
    try:  
        URL=chainurl+'/gettxncounterusers/'
        data ={
            "username": username,
            "count_date": count_date
            }
        print(data)
        headers = {"Content-Type": "application/json; charset=utf-8"}                 
        response = requests.post(URL, json=data, headers=headers).json()       
        print("response:{}".format(response))
        return response
    except Exception as e:       
        print(e)
        print("error while calling API".format(chainurl))  

# Dictionary Format

def dictfetchall(cursor):
    "Return all rows from a cursor as a dict"
    columns = [col[0] for col in cursor.description]
    #print(columns)
    return [
        dict(zip(columns, row))
        for row in cursor.fetchall()
    ]

# As Per Indian Format

def formatINR(number):
    s, *d = str(number).partition(".")
    r = ",".join([s[x-2:x] for x in range(-3, -len(s), -2)][::-1] + [s[-3:]])
    return "".join([r] + d)


# Display the table rows

def display_table_format(sm_state_name,dm_dapartment_name,CertName,create=0,update=0,read=0,verifier_reads=0):
    bodytemp='''\
                    <tr style="border-style: solid; border-color: Black; border-width: 1px">
                    <td align="left" style="background-color: white; text-align: left; width: 10%;" 
                        valign="bottom" class="style3">
                        {sm_state_name} </td>
                    <td align="left" style="background-color: white; text-align: left; width: 25%" 
                        valign="bottom" class="style3">
                        {dm_dapartment_name} </td>
                    <td align="left" style="background-color: white; text-align: left; width: 25%" 
                        valign="bottom" class="style3">
                        {CertName} </td>
                    <td align="left" style="background-color: white; text-align: right; width: 10%" 
                        valign="bottom" class="style3">
                        {create}</td>
                    <td align="left" style="background-color: white; text-align: right; width: 10%" 
                        valign="bottom" class="style3">
                        {update}</td>
                    <td align="left" style="background-color: white; text-align: right; width: 10%" 
                        valign="bottom" class="style3">
                        {read}</td>
                    <td align="left" style="background-color: white; text-align: right; width: 10%" 
                        valign="bottom" class="style3">
                        {verifier_reads}</td>
                    </tr>\
                    '''.format(sm_state_name=sm_state_name, dm_dapartment_name=dm_dapartment_name, CertName=CertName, create=formatINR(int(create)), update= formatINR(int(update)), read= formatINR(int(read)),verifier_reads= formatINR(int(verifier_reads)))
    return bodytemp

def totals(Create,Update,DeptReads,VerifierReads):
    total='''\
            <tr style="border-style: solid; border-color: Black; border-width: 1px">
            <td colspan="3"  align="left" style="background-color: white; text-align: right; width: 10%;" 
            valign="bottom" class="style3">
            <b>Total</b></td>                
            <td align="left" style="background-color: white; text-align: right; width: 10%" 
            valign="bottom" class="style3">
            <b>{create}</b></td>
            <td align="left" style="background-color: white; text-align: right; width: 10%" 
            valign="bottom" class="style3">
            <b>{update}</b></td>
            <td align="left" style="background-color: white; text-align: right; width: 15%" 
            valign="bottom" class="style3">
            <b>{read}</b></td>
            <td align="left" style="background-color: white; text-align: right; width: 15%" 
            valign="bottom" class="style3">
            <b>{verifier_reads}</b></td>
            </tr>\
            '''.format(create=formatINR(int(Create)), update= formatINR(int(Update)), read = formatINR(int(DeptReads)),verifier_reads = formatINR(int(VerifierReads)) )
    
    return total

