#!/usr/bin/env python
# coding: utf-8

# In[258]:


from __future__ import print_function

import   chargeback_rpt.report_builder_costing as costing_rpt
import   chargeback_rpt.report_builder_billing as billing_rpt
import chargeback_rpt.vm_data_charger as charger
import chargeback_rpt.vm_data_utility as vm_util
import chargeback_rpt.email_notifier as x_mail
import chargeback_rpt.vm_data_validator as vx
import chargeback_rpt.file_directory_manager as fd_mn
import datetime
import pandas as pd

import os
import sys


# In[259]:



from jinja2 import Environment, FileSystemLoader

from weasyprint import HTML




def  create_monthly_report(month_param,year_param,bill_info_sr=None,is_monthly=True):


    # # Start Transaction

    # In[262]:


    t_id=vm_util.creating_transaction(5,month_param,year_param)
    print(f"Transaction ID: {t_id}")

    list_error=[]
    print(list_error)

    # Move  to util
    def check_error_point(tran_id,is_mail_module=True):
      print(list_error)
      if True in list_error:

        # if errror was given from sub-system, we will let sub-system send error eamail by themselves
        if  is_mail_module==True:
         vm_util.collect_error_to_sent_mail(tran_id)

        print("Building report occured error")
        raise Exception("Program is teminated and check error from email and log_error.txt")

      list_error.clear()
      print(list_error)


    # # Check proper data in order to enable buiding report

    # In[263]:


    print("Check proper data in order to enable the system to buid report")
    def error_text(product, table,type_error):

        if  type_error==1:
         return f'{product} error due to Either no the complete job OR no row in {table} For  Month-{month_param} and Year-{year_param}.<br>'
        else:
         return f'{product} error due to no the complete job For  Month-{month_param} and Year-{year_param}.<br>'

    def check_valid_data_to_build_report():
        list_errors=[]

        # vm
        vm_jobType=1
        vm_job=charger.get_last_complete_job(month_param,year_param,vm_jobType)
        print('VM Complete Job\n',vm_job)
        if (vm_job is not None) and (len(vm_job)>0):
            vm_row=charger.check_existing_one_info('report_vm_info',month_param,year_param)
            print(f'Found at lest one row in VM For  Month-{month_param} and Year-{year_param}')
            list_errors.append(None);
        else:
           list_errors.append(error_text('VM','report_vm_info',1));

        print("========================================================")

        netapp_jobType=2
        netapp_job=charger.get_last_complete_job(month_param,year_param,netapp_jobType)
        print('NetApp Complete Job\n',netapp_job)
        if (netapp_job is not None) and (len(netapp_job)>0):
            netapp_row=charger.check_existing_one_info('report_storage_info',month_param,year_param)
            print(f'Found at lest one row in NetApp For  Month-{month_param} and Year-{year_param}')
            list_errors.append(None);
        else:
           list_errors.append(error_text('NetApp','report_storage_info',1));
        print("========================================================")

        nim_jobType=6
        nim_job=charger.get_last_complete_job(month_param,year_param,nim_jobType)
        print('Nimble Complete Job\n',nim_job)
        if (nim_job is not None) and (len(nim_job)>0):
            nim_row=charger.check_existing_one_info('report_nimble_info',month_param,year_param)
            if nim_row==True:
               print(f'Found at lest one row in Nimble For  Month-{month_param} and Year-{year_param}')
            else:
               print(f'Not found at lest one row in Nimble For  Month-{month_param} and Year-{year_param}')
            list_errors.append(None);
        else:
           list_errors.append(error_text('Nimble','report_nimble_info',2));

        print("========================================================")

        prim_jobType=7
        prim_job=charger.get_last_complete_job(month_param,year_param,prim_jobType)
        print('Primera Complete Job\n',prim_job)
        if (prim_job is not None) and (len(prim_job)>0):
            prim_row=charger.check_existing_one_info('report_primera_info',month_param,year_param)
            if prim_row==True:
               print(f'Found at lest one row in Primera For  Month-{month_param} and Year-{year_param}')
            else:
               print(f'Not found at lest one row in Primera For  Month-{month_param} and Year-{year_param}')
            list_errors.append(None);
        else:
           list_errors.append(error_text('Primera','report_primera_info',2));

        print("========================================================")




        return list_errors

    listxxx=check_valid_data_to_build_report()
    list_errors= [ error for error in listxxx if  error is not None ]
    if len(list_errors)>0:
     for error in list_errors:
        vm_util.add_error_to_database(24,error,t_id)

     vm_util.collect_error_to_sent_mail(t_id)
     print("Building report occured error")
     raise Exception((' ').join(list_errors))
     quit()




    # In[264]:


    str(list_errors)


    # In[265]:


    def clear_folder_after_complete(x_is_completed,x_is_monthly,x_zip_folder_path,x_report_path):

     fd_mn.delete_entire_directory(x_zip_folder_path)

     if x_is_monthly == False:
      fd_mn.delete_entire_directory(x_report_path)
     else:
       if x_is_completed ==False    :
         fd_mn.delete_entire_directory(x_report_path)


    # # load configuation data  by getting from database and Initialize  value

    # In[266]:


    try:
        print("Load configuation data  by getting from database and Initialize  value ")
        type_to_zip=vm_util.get_config_value('report_file_type',t_id).split(',')
        print(type_to_zip)

        temp_path=vm_util.get_config_value('temp_path',t_id)
        #ok,error=vx.check_existing_filepath(temp_path,t_id)
        print(temp_path)


        if is_monthly == True:
         report_rootpath = vm_util.get_config_value('report_rootpath',t_id)
         report_folder=f'{month_param}-{year_param}'
         zip_filename= f'chargeback_report_{month_param}-{year_param}.zip'
        else:
         report_rootpath =temp_path
         report_folder=f"report_temp_{datetime.datetime.now().strftime('%d-%m-%y_%H%M%S')}"
         zip_filename= f"chargeback_report_{month_param}-{year_param}_{datetime.datetime.now().strftime('%d-%m-%y_%H%M%S')}.zip"

        print(report_rootpath)
        print(report_folder)
        print(zip_filename)

        overview_templateName=vm_util.get_config_value('overview_report_template_path',t_id)
        cc_templateName=vm_util.get_config_value('cost_center_report_template_path',t_id)

        css_file=vm_util.get_config_value('css_file_path',t_id)
        img_file=vm_util.get_config_value('logo_png_path',t_id)




        print(overview_templateName)
        print(cc_templateName)
        print(css_file)
        print(img_file)

        vm_sheet_name =vm_util.get_config_value('vm_sheet_name',t_id)
        backup_sheet_name =vm_util.get_config_value('backup_sheet_name',t_id)
        netapp_sheet_name =vm_util.get_config_value('netapp_sheet_name',t_id)
        nimble_sheet_name =vm_util.get_config_value('nimble_sheet_name',t_id)
        primera_sheet_name =vm_util.get_config_value('primera_sheet_name',t_id)


    except Exception as ex:
        list_error.append(True)
        print(str(ex))
        vm_util.add_error_to_database(3,str(ex),t_id)



    # In[267]:


    check_error_point(t_id)


    # # Create report folder & file Name

    # In[268]:


    try:

        print("Create report folder & file Name")
        report_path=os.path.join(report_rootpath,report_folder)
        print(report_path)
        fd_mn.create_directory(report_path)

        filename_detail=f'Chargeback_Detail_{month_param}-{year_param}.xlsx'
        file_detail_report=os.path.join(report_path,filename_detail)
        print(file_detail_report)


        filename_summary=os.path.join(report_path,f'Chargeback_Summary_{month_param}-{year_param}.xlsx')
        file_summary_report=os.path.join(report_path,filename_summary)
        print(file_summary_report)

        overview_pdf=os.path.join(report_path,f'Overview_Chargeback_{month_param}-{year_param}.pdf')
        print(overview_pdf)

    except Exception as ex:
        list_error.append(True)
        print(str(ex))
        vm_util.add_error_to_database(21,str(ex),t_id)

    check_error_point(t_id)



    # # Get detail report data as Module

    # # Create Detail Report as Excel file

    # In[269]:


    try:
     print("Get detail report data as Module")
     detail_dict=costing_rpt.create_costing_detail(month_param,year_param)

    except Exception as ex:
     list_error.append(True)
     print(str(ex))


    check_error_point(t_id,False)


    # In[270]:


    try:
        print("Create Detail Report as Excel file")
        writer=pd.ExcelWriter(file_detail_report,engine='xlsxwriter')

        df_vm=detail_dict['vm_detail']
        df_vm.to_excel(writer, sheet_name=vm_sheet_name,index=False)


        df_storage=detail_dict['storage_detail']
        if df_storage is not None:
           df_storage.to_excel(writer, sheet_name=netapp_sheet_name,index=False)

        df_nimble=detail_dict['nimble_detail']
        if df_nimble is not None:
           df_nimble.to_excel(writer, sheet_name=nimble_sheet_name,index=False)

        df_primera=detail_dict['primera_detail']
        if df_primera is not None:
          df_primera.to_excel(writer, sheet_name=primera_sheet_name,index=False)

        ref_master=detail_dict['cost_ref_master']
        ref_fixed=detail_dict['cost_ref_fixed']
        ref_addtion=detail_dict['cost_ref_addition']


        ref_master.to_excel(writer,sheet_name='main_cost',index=False)
        ref_fixed.to_excel(writer,sheet_name='fixed_cost',index=False)
        ref_addtion.to_excel(writer,sheet_name='category_cost',index=False)

        writer.save()


    except Exception as ex:
        list_error.append(True)
        print(str(ex))
        vm_util.add_error_to_database(24,str(ex),t_id)


    # In[271]:


    check_error_point(t_id)


    # # Set Billing Data Info

    # In[272]:


    print("Set Billing date info")
    if  bill_info_sr is None:
        bill_info_sr=vm_util.get_initial_bill_info(int(month_param),int(year_param),t_id)

    print(bill_info_sr)


    # In[273]:


    def update_hearder_report_dict(x_dict):
        x_dict['Report_Logo']=img_file
        x_dict['BillInfo']= bill_info_sr
        return x_dict


    # # Get billing report data as Module

    # In[274]:


    try:

        print("Get billing report data as Module")
        overview_dict,cc_dictList=billing_rpt.create_billing_report(detail_dict,month_param,year_param)
        #overview_dict,cc_dictList=billing_rpt.create_billing_report_pysical(df_vm,df_storage,month_param,year_param)
        overview_dict=update_hearder_report_dict(overview_dict)
        print(overview_dict.keys())

    except Exception as ex:

        list_error.append(True)
        print(str(ex))


    check_error_point(t_id,False)


    # # Fillter No-Cost of CostType to not show in Overview Report
    #

    # In[275]:


    print ("Fillter only have cost to show in Overview Report ")
    costDF_filterOut=['VM_Cost','Backup_Cost','Storage_Cost','Nimble_Cost','Primera_Cost']

    for cost_key in costDF_filterOut:
        if  overview_dict[cost_key] is not None:
           df=overview_dict[cost_key]
           if df.empty==True:
             overview_dict[cost_key]=None
             print(f'{cost_key} dataframe is empty')
           else:
            sum_cost=df.iloc[:,2].sum()
            if sum_cost==0  :
              overview_dict[cost_key]=None
              print(f'{cost_key} dataframe amount  is  0')
            else:
              print(f'{cost_key} dataframe amount  is  {sum_cost}')



    # # Create Billing Report as Pdf file and Excel Summary

    # In[276]:


    if   float(overview_dict['Total_AllCost'])>0:

        print("Print Overview Report")
        overview_html=vm_util.get_content_template(overview_templateName,overview_dict, tran_id=t_id)
        try:
         overview_html=vm_util.get_content_template(overview_templateName,overview_dict, tran_id=t_id)
         HTML(string=overview_html).write_pdf(overview_pdf,stylesheets=[css_file])
        #print(overview_html)
        except Exception as ex:
         list_error.append(True)
         print(str(ex))
         vm_util.add_error_to_database(24,str(ex),t_id)
    else:
        print("No report")

    check_error_point(t_id)


    #print(overview_html)


    # In[277]:


    try:
        print("Create Report Summary as Excel file")
        writer=pd.ExcelWriter(filename_summary,engine='xlsxwriter')

        df_vm_sum=overview_dict['VM_Cost']
        if  df_vm_sum is not None  :
         df_vm_sum.to_excel(writer, sheet_name=vm_sheet_name,index=False)

        df_backup_sum=overview_dict['Backup_Cost']
        if df_backup_sum is not None:
         df_backup_sum.to_excel(writer, sheet_name=backup_sheet_name,index=False)


        df_storage_sum=overview_dict['Storage_Cost']
        if df_storage_sum is not None:
           df_storage_sum.to_excel(writer, sheet_name=netapp_sheet_name,index=False)

        df_nim_sum=overview_dict['Nimble_Cost']
        if df_nim_sum is not None:
           df_nim_sum.to_excel(writer, sheet_name=nimble_sheet_name,index=False)

        df_prim_sum=overview_dict['Primera_Cost']
        if df_prim_sum is not None:
           df_prim_sum.to_excel(writer, sheet_name=primera_sheet_name,index=False)

        writer.save()


    except Exception as ex:
        list_error.append(True)
        print(str(ex))
        vm_util.add_error_to_database(24,str(ex),t_id)


    # print("Print Cost Center Report")

    # In[278]:


    try:

     for cc in cc_dictList:
       for key,item in cc.items():

         cc_name=key
         print("###############################################################################")
         print(f'CostCenter : {cc_name}')

         if float(item['Total_AllCost'])>0:

           for cost_key in costDF_filterOut:
             if item[cost_key] is not None:
                df=item[cost_key]
                if df.empty==True:
                  item[cost_key]=None
                else:
                  sum_cost=df.iloc[:,2].sum()
                  if sum_cost==0  :
                    item[cost_key]=None


           print('CostbyCate', item['Total_ByCateCost'])
           for cost_key in costDF_filterOut:
             print (item[cost_key])
           print("###############################################################################")

           template_vars_cc=update_hearder_report_dict(item)
           #print(template_vars_cc.keys())
           pdf_name_cc=os.path.join(report_path,f'{cc_name}_Chargeback_{month_param}-{year_param}.pdf')
           print(pdf_name_cc)
           cc_html=vm_util.get_content_template(cc_templateName,template_vars_cc, tran_id=t_id)
           HTML(string=cc_html).write_pdf(pdf_name_cc,stylesheets=[css_file])

    except Exception as ex:
         list_error.append(True)
         print(str(ex))
         vm_util.add_error_to_database(24,str(ex),t_id)

    check_error_point(t_id)


    # # Create Zip Tile

    # In[279]:


    try:

        zip_folder_name=f"zip_report_{datetime.datetime.now().strftime('%d-%m-%y_%H%M%S')}"
        zip_folder_path = os.path.join(temp_path,zip_folder_name)


        result_os=fd_mn.create_directory(zip_folder_path)

        zip_temp_file = os.path.join(zip_folder_path,zip_filename)
        type_to_zip=['.pdf','.xlsx']

        print(zip_temp_file)
        result_os=fd_mn.make_zip(report_path,type_to_zip,zip_temp_file)
        print(result_os)

    except Exception as ex:
         list_error.append(True)
         print(str(ex))
         vm_util.add_error_to_database(24,str(ex),t_id)


    check_error_point(t_id)


    # # Email Notification
    #

    # # Send mail as Module

    # In[280]:


    print("Send mail")
    #required_keys=['email_type','transaction_id','attached_file_path','content_data_dict']
    try:
     content_data_dict= {
                     "ContentTitle":"List All Cost-Center Report in AttachedFile",
                    }
     ok= x_mail.send_email(email_type='report',transaction_id=t_id,
                     attached_file_path=zip_temp_file,content_data_dict=content_data_dict)

    except Exception as ex:
     print(str(ex))
     list_error.append(True)
     # delelte all folder
     clear_folder_after_complete(False,is_monthly,zip_folder_path,report_path)


    check_error_point(t_id,False)



    # # Delete temp folder report (AC-hoc)
    #

    # In[281]:


    print("Delete temp folder and zip report")
    clear_folder_after_complete(True,is_monthly,zip_folder_path,report_path)


    # # Completed Transaction

    # In[282]:


    try:

        updated_rows=vm_util.created_transaction(t_id)
        print("completed building report")

    except Exception as ex:
        list_error.append(True)
        print(ex)

    check_error_point(t_id)


    # In[ ]:





    # In[ ]:




