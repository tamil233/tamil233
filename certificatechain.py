from datetime import date, time,timedelta
import requests
import psycopg2
import pymssql
import os
import constants
import json
from zeep import Client
import base64
from pathlib import Path
import functions




def main():
    try:
        #print("Document fabric onboarded users")
        current_date=  date.today()
        current_date_str= str(current_date)
        yesterday_date = current_date - timedelta(days=1)
        yesterday_date_str_1=yesterday_date.strftime("%d-%m-%Y")
        print(yesterday_date)
        yesterday_date_str=yesterday_date.strftime("%Y-%m-%d")
        delete_file_date = current_date - timedelta(days=2)
        delete_file_date_str=delete_file_date.strftime("%d-%m-%Y")
        print(yesterday_date_str)
        mail_json={}
        nic_body=""
        bcerror="<h3>Block Chain Exceptions</h3></br>"
        totalDeptReads = 0
        totCreate = 0 
        totUpdate = 0
        totVerifierReads = 0
        chain_url = constants.cert_url
        # Get the state List
        statelist = functions.getState(chain_url)
        
        for i in statelist: 
            totalStateDeptReads = 0
            totStateCreate = 0 
            totStateUpdate = 0
            totStateVerifierReads = 0
            state_mnemonic = i["state_mnemonic"]
            # Get the Onboarded Users
            userslist = functions.getOnboardedUsers(chain_url,state_mnemonic)
            print("users onboarded:{}".format(userslist))
            for i in userslist:  
                username = i["username"]
                body=""        
                # Get the Transactions specific to user      
                txncounter = functions.getTxnCounterUsers(chain_url,username, yesterday_date_str)
                print("txncounter:{}".format(txncounter))
                for txnlist in txncounter:
                    # Get read by Verifier
                    print("txnlist:{}".format(txnlist))                                                                                                                                                                                                                                                         
                    verifier_reads = functions.getReadsByVerifier(chain_url,txnlist["state_mnemonic"], txnlist["department_mnemonic"], txnlist["certtype_mnemonic"],yesterday_date_str)   
                    print("verifier_reads:{}".format(verifier_reads))
                    # Get Document Type
                    doc_type_name = functions.getCertificateType(txnlist["state_mnemonic"], txnlist["department_mnemonic"], txnlist["certtype_mnemonic"])

                    print("getCertificateType:{}".format(doc_type_name))
                    sm_state_name = doc_type_name[0]["StateName"]
                    dm_dapartment_name = doc_type_name[0]["DeptName"]
                    CertName = doc_type_name[0]["CertName"] 

                    # adding table template code of each user respective state.
                    bodytemp= functions.display_table_format(sm_state_name,dm_dapartment_name,CertName,txnlist["insert_counts"],txnlist["update_counts"],txnlist["select_counts"],verifier_reads)
                    body += bodytemp

                    totStateCreate += int(txnlist["insert_counts"])
                    totStateUpdate += int(txnlist["update_counts"])
                    totalStateDeptReads += int(txnlist["select_counts"])
                    totStateVerifierReads += int(verifier_reads)
                    totCreate += int(txnlist["insert_counts"])
                    totUpdate += int(txnlist["update_counts"])
                    totalDeptReads += int(txnlist["select_counts"])
                    totVerifierReads += int(verifier_reads)
                nic_body += body
                emailid = i["email"]

                subject="Certificate Chain Statistics for "+yesterday_date_str_1
                fname = "emailtemplate_nic.htm"
                html_file = open(fname, 'r', encoding='utf-8')
                source_code = html_file.read() 
                source_code = source_code.replace("@chainname", 'Certificate Chain')
                source_code = source_code.replace("@body", body)
                source_code = source_code.replace("@username", username)
                source_code = source_code.replace("@date", yesterday_date_str_1)

                source_code_string_bytes = source_code.encode("ascii")
  
                base64_bytes = base64.b64encode(source_code_string_bytes)
                base64_string = base64_bytes.decode("ascii")
                mail_json[emailid+'|'+subject] = base64_string

            # Function for statetotal
            statetotal= functions.totals(totStateCreate,totStateUpdate,totalStateDeptReads,totStateVerifierReads)
            nic_body += statetotal

        # Function for grand total
        grandtotal = functions.totals(totCreate,totUpdate,totalDeptReads,totVerifierReads)    
        #print(grandtotal)
        nic_body += grandtotal
        #totalTxn = getTotalReads(yesterday_date)
        #print(nic_body)
        #totalReads = totalTxn[0]["select_counts"]
        #totalReadsByOthers= totalReads - totalDeptReads
        #print(totalReadsByOthers) 
        errorLogs = functions.getErrorLogs(chain_url,yesterday_date_str)
        #errorLogs = "NotExist" 
        print("errorlogs=="+errorLogs)
        if errorLogs == "NotExist" : 
            bcerrortemp="None"
            bcerror += bcerrortemp  
        else:
            for i in errorLogs:
                remarks = i["remarks"]
                bcerrortemp = '''           
                <p>{remarks}</p></n>
                \
                '''.format(remarks = remarks)
                bcerror += bcerrortemp       
            
        fname = "emailtemplate_nic.htm"
        html_file = open(fname, 'r', encoding='utf-8')
        source_code = html_file.read()
        source_code = source_code.replace("@chainname", 'Certificate Chain') 
        source_code = source_code.replace("@body", nic_body)
        source_code = source_code.replace("@username", "All")
        source_code = source_code.replace("@date", yesterday_date_str_1) 
        source_code = source_code.replace("@Blockchain", bcerror)
        #base 64 encode
        source_code_string_bytes = source_code.encode("ascii")
  
        base64_bytes = base64.b64encode(source_code_string_bytes)
        base64_string = base64_bytes.decode("ascii")
  
        print(f"Encoded string: {base64_string}")

        #base64 decode
        base64_bytes = base64_string.encode("ascii")
  
        sample_string_bytes = base64.b64decode(base64_bytes)
        sample_string = sample_string_bytes.decode("ascii")
        
        print(f"Decoded string: {sample_string}")
        
        
        #json_data = json.dumps(data)
        #print("json_data"+json_data)
        
        
        
        mail_json['s.jayanthi@nic.in,tpmuthu@nic.in,krishna.gs@nic.in,santanu@nic.in,agarwal.mini@nic.in,rajammal.t@nic.in,p.sumanth@nic.in,chethankumark44@gmail.com,veereshkdcr7@gmail.com,nicdbtshri@gmail.com'+'|'+subject] = base64_string
        #mail_json['agarwal.mini@nic.in,p.sumanth@nic.in,chethankumark44@gmail.com'+'|'+subject] = base64_string
        print("email:{}".format(mail_json))
        print("deleteddate:{}".format(delete_file_date_str))
        dir_path = Path('G:/email_scheduler/email_scheduler/schedulers')
        file_name = 'certchain_mail_'+yesterday_date_str_1+'.json'
        if dir_path.is_dir():
            with open (dir_path.joinpath(file_name),'w') as f:
                f.write(json.dumps(mail_json))
        else:
            print('Directory doesn\'t exist')
                            	
        delete_file_name='docchain_mail_'+delete_file_date_str+'.json'
        delete_file_path= 'G:/email_scheduler/email_scheduler/schedulers'+'/' + delete_file_name
        check_file = os.path.exists(delete_file_path)
        if(check_file):
            os.remove(delete_file_path)



                                    	
            
        #mail_json['sumanth@nic.in'] = base64_string
        
        #json_data = json.dumps(mail_json)
        #print("json_data"+json_data)
    except Exception as e:
        print(e)
        return "SQL Error"


if __name__ == '__main__':
    main()
