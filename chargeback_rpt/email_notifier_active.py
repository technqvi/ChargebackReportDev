
import email, smtplib, ssl, os, sys, shutil

import pandas as pd

from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from socket import gaierror

import datetime
import chargeback_rpt.vm_data_utility as vm_util
import chargeback_rpt.db_postgres_command as db_command



def send_email(**kwargs):
    # opion_keys=['email_subject']
    required_keys=['email_type','transaction_id','attached_file_path','content_data_dict']
    server_info={'email_server':None,'email_port':None,'email_user':None,'email_password':None}
    try:
     key_not_found=[]
     email_data=kwargs

     for key in required_keys:
      if key not in email_data:
       key_not_found.append(key)

     if  len(key_not_found)>0:
      raise Exception(f'not found these info for sending email:{required_keys}')
     else:
      email_type=email_data['email_type']
      t_id=email_data['transaction_id']
      attached_file_path=email_data['attached_file_path']
      content_data_dict=email_data['content_data_dict']

    except Exception as ex:
     if  'transaction_id' in  key_not_found:
      vm_util.add_error_to_database(23,str(ex),None)
     else:
      vm_util.add_error_to_database(23,str(ex),email_data['transaction_id'])

     raise Exception(ex)





    # In[14]:


    def get_email_subject(email_type,title):

      x_datenow=datetime.datetime.now()

      new_title=title

      if  email_type=='error':
       timestamp=datetime.datetime.now().strftime('%d-%m-%y_%H%M')
       new_title=f"{title} {timestamp}"

      else :
       month_param=x_datenow.strftime('%B')
       year_param=x_datenow.strftime('%Y')
       new_title=f'{title} {month_param}-{year_param}'

      return new_title





    # In[15]:


    def get_mail_server(server_info,tran_id=None):

     for key,value in server_info.items():
        val_sr=vm_util.get_value_by_key(key)
        #print(val_sr['value'])
        if (val_sr is not None)  :
           server_info[key]=val_sr['value']
        else:
           error_des=f'no key:{key} in key column for  email-server info in config_value table'
           vm_util.add_error_to_database(3,error_des,tran_id)
           raise Exception(error_des )

     return server_info


    # In[16]:


    def set_mail_connection(ip,port):
        if (ip is not None) and  (port is not None):
          return smtplib.SMTP(ip,port)
        elif (ip is not None) and  (port is  None):
          return smtplib.SMTP(ip)
        else:
          raise Exception("not allow ip is none" )



    # In[17]:


    def get_email_by_type(type,tran_id=None):
       sql="select * from email_notification_type where email_type=(%s)"
       paramx=(type,)
       valx=db_command.get_one_sql(db_command.get_postgres_conn(),sql,paramx)
       if(valx is not None):
        sr=pd.Series(valx)
        return sr
       else:
        error_des=f'not found email_type={type} in email_notification_type table'
        vm_util.add_error_to_database(22,str(error_des),tran_id)
        raise Exception(error_des)


    # In[18]:


    #def attach_multiplefiles(file_path)
    def attach_singlefile(file_path):

     try:
      with open(file_path, "rb") as attachment:

        part = MIMEBase("application", "octet-stream")
        part.set_payload(attachment.read())

        encoders.encode_base64(part)

        file_name=os.path.basename(file_path)
        part.add_header(
        "Content-Disposition",
        f"attachment; filename= {file_name}",)

        return part

     except Exception as error:
       raise Exception(error )


    # # Get Email Data from DB

    # In[19]:


    # get from database
    server_info=get_mail_server(server_info,t_id)
    #print(server_info)

    host = server_info['email_server']
    port = server_info['email_port']
    uid = server_info['email_user'] # paste your login generated by Mailtrap
    password =server_info['email_password'] # paste your password generated by Mailtra

    print(f'server={host} , port={port} , username={uid},pwd={password}')


    # In[20]:



    sr=get_email_by_type(email_type,t_id)
    for key,item in sr.iteritems():
     print(f"{key} = {item}")

    x_predix_subject=sr['prefix_subject']

    # if found  email_data['email_subject'] use subject with this
    x_subject = get_email_subject(email_type, sr['subject'])



    x_sender=sr['sender']
    x_receivers =sr['receivers']
    x_ccs =sr['CCs']
    x_email_template=sr['template_content']

    list_receivers=x_receivers.split(',')

    if x_ccs is not None:
     list_CCs = x_ccs.split(',')
    else:
     list_CCs=None

    print(list_receivers)
    print(list_CCs)


    # # Compose Mail Message Content

    # In[21]:


    message = MIMEMultipart("alternative")

    message_subject_email=f'{x_predix_subject}:{x_subject}'
    message["Subject"] = message_subject_email

    message["From"] = x_sender
    message["To"] = x_receivers

    if list_CCs is not None:
     message["Cc"] = x_ccs


    print(message["From"])
    print(message["To"])
    print(message["Cc"])


    # In[22]:

    if attached_file_path is not None:
        path_attachment = attach_singlefile(attached_file_path)
        message.attach(path_attachment)
    # print(path_attachment)


    # In[23]:


    html_output=vm_util. get_content_template(x_email_template,content_data_dict,t_id)

    message.attach(MIMEText(html_output , "html"))
    #print(html_output)


    # In[24]:


    try:

        with set_mail_connection(host,port) as mail_server:
            if uid  is not None and password  is not None:
              mail_server.login(uid, password)
            if list_CCs is None:
             mail_server.sendmail(x_sender, list_receivers, message.as_string())
            else:
             mail_server.sendmail(x_sender, list_receivers+list_CCs, message.as_string())

            print("Successfully sent email")


    except Exception as e:
        error_title=f"Cannot sent email on subject {message_subject_email}"
        error_des=f'{error_title} : Some errors were found '
        error_des=error_des+str(e)
        vm_util.add_error_to_database(23,error_des,t_id)

        raise Exception(error_des )



    # In[ ]:


    return True






