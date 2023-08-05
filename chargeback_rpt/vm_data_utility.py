import chargeback_rpt.db_postgres_command as db_command
import chargeback_rpt.file_directory_manager as fd_mn
import json
import pandas as pd

import bitmath
import datetime
from dateutil.relativedelta import relativedelta
import calendar

import os
import sys
from jinja2 import Environment, FileSystemLoader

import numpy as np
import numpy as np



def get_initial_bill_info(month_x,year_x, t_id):
    try:

        bill_date_from = int(get_config_value("bill_date_from", None))
        bill_date_to = int(get_config_value("bill_date_to", None))
        costPeriod_month = int(get_config_value("bill_costPeriod_month", None))

        # set the first data of month tempolarily
        xFirstDate = datetime.datetime(year_x, month_x, 1)


        # crete bill data from ==> to
        xPrevMonth = xFirstDate - relativedelta(months=costPeriod_month)
        billFromDate = datetime.datetime(xPrevMonth.year, xPrevMonth.month, bill_date_from)
        billToDate = datetime.datetime(year_x, month_x, bill_date_to)

        invoice_date_at = int(get_config_value("invoice_date_at", None))
        duePeriod_month = int(get_config_value("invoice_duePeriod_month", None))

        # invoice date and due data data
        # 0 is last daty of the month
        # invoice due data for payment
        if invoice_date_at == 0:
            invoiceDate = datetime.datetime(year_x, month_x, calendar.monthrange(year_x, month_x)[1])
            xNextMonth = xFirstDate + relativedelta(months=duePeriod_month)
            invoiceDate_due = datetime.datetime(xNextMonth.year, xNextMonth.month,
                                                calendar.monthrange(xNextMonth.year, xNextMonth.month)[1])
        else:
            invoiceDate = datetime.datetime(year_x, month_x, invoice_date_at)
            invoiceDate_due = invoiceDate + relativedelta(months=duePeriod_month)

        bill_info_sr = pd.Series({'bill_from': billFromDate, 'bill_to': billToDate,
                                  'bill_invoice': invoiceDate, 'bill_due': invoiceDate_due,
                                   'bill_mm_yy':xFirstDate}
                                 )

        return bill_info_sr

    except Exception as ex:
        add_error_to_database(26, str(ex), t_id)
        print(str(ex))
        raise ex


def move_files_after_finished_ETL():
    #chnage file name with dd_mm_yy_hh_mm_ss
    # move file to datetimed folder
    # https://www.w3schools.com/python/python_file_remove.asp

    return None



def convert_Xb_unit_Gb(source_value, source_unit_name):
    "First param is value as float type   and the second param its unit name such askb,mb,gb, return  is converted value to GB"
    #https://www.calculatorsoup.com/calculators/conversions/computerstorage.php
    #KB or Kb   bitmath.KiB()   or  Mib


    # check from https://convertlive.com/u/convert/bytes/to/gigabytes
    if source_unit_name.lower() == 'byte':
        s_value = bitmath.Bit(source_value)

    elif source_unit_name.lower() == 'kb':
        s_value = bitmath.Kib(source_value)
        #s_value = bitmath.KiB(source_value)

    elif source_unit_name.lower() == 'mb':
        s_value = bitmath.Mib(source_value)
        #s_value = bitmath.MiB(source_value)

    elif source_unit_name.lower() == 'gb':
        s_value = bitmath.Gib(source_value)
        #s_value = bitmath.GiB(source_value)
    elif source_unit_name.lower() == 'tb':
        s_value = bitmath.Tib(source_value)
        #s_value = bitmath.TiB(source_value)
    elif source_unit_name.lower() == 'pb':
        s_value = bitmath.Pib(source_value)
    elif source_unit_name.lower() == 'eb':
        s_value = bitmath.Eib(source_value)
    # elif source_unit_name.lower() == 'zb':
    #     s_value = bitmath.Zb(source_value)
    # elif source_unit_name.lower() == 'yb':
    #     s_value = bitmath.Yb(source_value)

    s_value = round(s_value.to_Gib().value,4)

    return s_value


def set_size_gb(size_value):
     value=convert_Xb_unit_Gb(size_value,'MB')
     return value



def add_error_to_file(error_des):
    "put error to  log file if database error"
    f = open(r'D:\ChargeBackApp\log_error.txt', 'a')
    error_str = f'{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}|{repr(error_des)}\n'

    f.write(error_str)
    f.close()
    print(error_str)
    raise Exception(error_str)


def add_error_to_specific_file(error_des,file_path):
    "put error to  log file any action"
    f = open(file_path, 'a')
    error_str = f'{datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S")}|{repr(error_des)}\n'

    f.write(error_str)
    f.close()
    print(error_str)
    raise Exception(error_str)

def add_error_to_database(type_id,error_des,transaction_id=None):
    "put error database"
    try:
     params = (type_id, error_des,datetime.datetime.now(),transaction_id)
     sql_insert="INSERT INTO error_log(type_id,des,created_date,transaction_id) VALUES(%s,%s,%s,%s) RETURNING id;"
     id=db_command.add_one_data_sql(db_command.get_postgres_conn(),sql_insert,params)
     if id is not None:
      return id

    except Exception as error:
        error_des=f'{str(error)} and {error_des}'
        add_error_to_file(error_des)


def creating_transaction(jobtype_id,month_x,year_x):
    params=(datetime.datetime.now(),jobtype_id,month_x,year_x)
    sql="INSERT INTO job_transaction(start_datetime,type_id,month,year) VALUES(%s,%s,%s,%s) RETURNING id;"
    id = db_command.add_one_data_sql(db_command.get_postgres_conn(), sql, params)
    return id

def created_transaction(transaction_id):
    params=(datetime.datetime.now(),True,transaction_id)
    sql="update  job_transaction set end_datetime=%s ,completed=%s where id=%s"
    updated_rows = db_command.update_data_sql(db_command.get_postgres_conn(), sql, params)
    return updated_rows

def delete_transcation(transaction_id):
    sql = "DELETE FROM report_vm_info WHERE transaction_id = %s"
    params = (transaction_id,)
    del_rw = db_command.delete_data_sql(db_command.get_postgres_conn(), sql, params)
    return del_rw


def list_transactions_by_id(transaction_id):
    try:
        sql="select * from job_transaction j inner join job_type jt on j.type_id=jt.id where j.id=(%s)"
        paramx = (transaction_id,)
        valx = db_command.get_one_sql(db_command.get_postgres_conn(), sql, paramx)
        if (valx is not None):
            sr = pd.Series(valx)
            return sr
        else:
            return None
    except Exception as error:
        # raise  error
        return None



def list_errors(transaction_id):
    try:
        sql = "select err.*,err_type.type,err_type.troubleshooting ,err.transaction_id from error_log err " \
              " inner join error_log_type err_type on err.type_id=err_type.id where err.transaction_id = (%s)"
        params = (transaction_id,)
        errors = db_command.get_list_sql(db_command.get_postgres_conn(), sql, params)
        if errors is not None:
           return pd.DataFrame(data=errors)
        else:
            return None
    except Exception as error:
        # raise  error
        return None




def get_value_by_key(keyx):
 try:
     sql="select value from config_value where key=(%s)"
     paramx=(keyx,)
     valx=db_command.get_one_sql(db_command.get_postgres_conn(),sql,paramx)
     if(valx is not None):
      sr=pd.Series(valx)
      return sr
     else:
      return None
 except Exception as error:
     # raise  error
      return None
def get_sourceFile_info(source_name):
 try:
    sr=None
    sql_cmd="select * from datasource_mapping where source_name =(%s)"
    sql_params=(source_name,)
    source_item=db_command.get_one_sql(db_command.get_postgres_conn(),sql_cmd,sql_params)
    if source_item is not None:
     sr=pd.Series(source_item)
     return sr
    else:
        return None
 except Exception as error:
     #raise  error
     return None



def get_active_datafield(datasource_id):
 try:
    # focusing on column_table_name,field_source_name
    sql_cmd="select * from datafield_mapping where datasource_id =(%s) and is_active=true"
    sql_params=(datasource_id,)
    list_field=db_command.get_list_sql(db_command.get_postgres_conn(),sql_cmd,sql_params)
    if list_field is  not None:
     df=pd.DataFrame(data=list_field)
     return df
    else:
     return None
 except Exception as errors :
     # raise  error
     return None

def get_xtable(tb_name, col_name):
 try:
    sql_str = "SELECT column_name  FROM information_schema.columns WHERE table_name = (%s) and column_name!='id' "
    sql_params = (tb_name,)
    cols = db_command.get_list_sql(db_command.get_postgres_conn(), sql_str, sql_params)
    if cols is not None:
      dfx = pd.DataFrame(data=cols)
      dfx.rename(columns={'column_name': col_name}, inplace=True)
      return dfx
    return None
 except  Exception as errors :
     return None

def verify_existing_columns(source_columns,target_columns):
    "Takes two  list to compare, all in target must be in source_columns"
    not_exist_col = [x for x in target_columns if x not in source_columns]
    if len(not_exist_col)>0:
        return f"Some columns in datafield_mapping table weren't found' in source file(transform from datasource to dataframe) as follows {not_exist_col}"
    else :
        return None


def create_extra_fields_byJson(df,json_colName='additional_info'):
    extra_fields = []
    def create_addtional_fields(ex_col):
        # prevent some loading  as string occationally
        # print(type(ex_col))
        if isinstance(ex_col, str):
            ex_col = json.loads(ex_col)
        # print(type(ex_col))

        for json_key in ex_col.keys():
            if json_key not in extra_fields:
                new_col = json_key
                extra_fields.append(new_col)

    def insert_value_to_addtional_column(ex_col, key_extra_f):
        # prevent some loading  as string occationally
        if isinstance(ex_col, str):
            ex_col = json.loads(ex_col)

        if key_extra_f in ex_col.keys():
            return ex_col[key_extra_f]
        else:
            return None

    df[json_colName].apply(create_addtional_fields)

    # check all caculatoin column include newly added extra field is in table mapling  again
    print('Addtional columns:', extra_fields)

    for item_f in extra_fields:
        df[item_f] = df[json_colName].apply(insert_value_to_addtional_column, args=(item_f,))

    return df

def get_content_template(template_fullpath, variable_data_dict, tran_id=None):
    try:
        path_name = os.path.split(template_fullpath)
        template_path = path_name[0]
        template_file = path_name[1]
        print(template_path)
        print(template_file)

        env = Environment(loader=FileSystemLoader(template_path))
        template = env.get_template(template_file)

        if variable_data_dict is not None:
            output = template.render(variable_data_dict)
        else:
            output = template.render()

        return output

    except Exception as error:
        error_des = f"not found template file {template_fullpath}"
        add_error_to_database(21, error_des, tran_id)
        raise error

# implement buid-on top  get_value_by_key
def get_config_value(key, transaction_id):
    # on top  get_value_by_key to ease caller
    value_sr = get_value_by_key(key)
    xvar = None
    if value_sr is not None:

        xvar = value_sr['value']

    else:

        error_message = f'no key:{key} in key column in config_value table'
        print(error_message)
        add_error_to_database(3, error_message, transaction_id)
        raise Exception(error_message)

    return xvar



def get_cost_center(cc_name,transaction_id):
 try:

     cc_name_low = cc_name.replace(' ', '').upper()
     sql="select * from cost_center where upper(cost_center_name)=(%s)"
     paramx=( cc_name_low,)
     valx=db_command.get_one_sql(db_command.get_postgres_conn(),sql,paramx)
     if(valx is not None):
      sr=pd.Series(valx)
      return sr
     else:
      error_message = f'not found:{cc_name}'
      print(error_message)
      add_error_to_database(25, error_message, transaction_id)
      raise Exception(error_message)
 except Exception as error:
      raise  error
      # return None

import chargeback_rpt.email_notifier as x_mail

def collect_error_to_sent_mail(tt_id):
    try:
        link_all_error = get_config_value("email_link_all_error", tt_id)
        link_detail_error = get_config_value("email_link_detail_error", tt_id)
        link_error_tran=get_config_value("email_link_error_tran",tt_id)

        tran_sr = list_transactions_by_id(tt_id)
        if tran_sr is not None:

            df_error = list_errors(tt_id)
            df_error = df_error[['id', 'des', 'type', 'created_date']]
            print(df_error)

            if df_error is not None:
                start_tm = (tran_sr['start_datetime'].replace(tzinfo=None)).strftime("%d-%b-%Y %H:%M")
                content_data_dict = {
                    "ContentTitle": f"<b><u>{tran_sr['name']}</u></b> List Error of TranID# <b><u>{tt_id}</u></b> at  <b><u>{start_tm}</u></b>",

                    "Len_Cols_ErrorList": len(df_error.columns),
                    "ErrorList": df_error,

                    "TranID": tt_id,
                    'LinkByTransation':link_error_tran,

                    'LinkAll': link_all_error,
                    'LinkEach': link_detail_error
                }

                ok = x_mail.send_email(email_type='error', transaction_id=tt_id,
                                       attached_file_path=None, content_data_dict=content_data_dict)

                print("The system got mail sent already")
                return ok


    except Exception as ex:
        print(str(ex))
        add_error_to_file(str(ex))
        raise ex


def finished_etl_folder(tran_id, etl_type, is_successful, etl_files):
    try:

        def create_folder_etl(root_key, t_id, status_name, type_name):
            time_stamp = datetime.datetime.now().strftime('%d%m%y_%H%M%S')

            etl_root_path = get_config_value(f'{root_key}', t_id)
            elf_folder_name = f"{t_id}_{time_stamp}_{status_name}_{type_name}"
            etl_folder_path = os.path.join(etl_root_path, elf_folder_name)

            ok = fd_mn.create_directory(etl_folder_path)
            return etl_folder_path, time_stamp

        if is_successful == True:
            xpath, xtime = create_folder_etl('import_path_completed', tran_id, is_successful, etl_type)
        else:
            xpath, xtime = create_folder_etl('import_path_failed', tran_id, is_successful, etl_type)

        print(f'Move to : {xpath}')
        for items in etl_files:
            olde_file_path = items[1]

            apath, afile = os.path.split(olde_file_path)
            aname, atype = os.path.splitext(afile)
            new_aname = f'{aname}_{tran_id}_{xtime}_{items[0]}{atype}'
            new_file_path = os.path.join(apath, new_aname)

            #        print(olde_file_path)
            #        print(new_file_path)
            ok = fd_mn.rename_file(olde_file_path, new_file_path)
            ok = fd_mn.move_file(new_file_path, xpath)
    except Exception as ex:
        error_message = str(ex)
        print(error_message)
        add_error_to_database(21, error_message, tran_id)
        raise ex


def add_new_cost_center(cost_centerList,process_name,tran_id):
   "cost center name in list"
   df_new_cc = None
   try:

        all_cost_center_key = 'email_link_all_cost_center'
        update_cost_center_key = 'email_link_update_cost_center'

        main_cc_name=get_config_value("main_cost_center",tran_id)
        main_cost_center=get_cost_center(main_cc_name,tran_id)

        new_ccList = []


        def insert_new_cost_center(cc_name):
            try:
                cc_name_low = cc_name.replace(' ', '').lower()

                sql = "select company_name from cost_center where lower(replace(cost_center_name, ' ', '')) = (%s) "
                param_x = (cc_name_low,)
                cc_item = db_command.get_one_sql(db_command.get_postgres_conn(), sql, param_x)


                if cc_item is None:
                    print('new cost-center : ', cc_name)
                    params = (cc_name, main_cost_center['company_name'], main_cost_center['company_address']
                              , main_cost_center['cost_center_owner'], main_cost_center['cost_center_owner_email'],)

                    # params = (cc_name, '',''
                    #           ,'', '',)

                    sql_insert = """INSERT INTO cost_center(cost_center_name,company_name,company_address,cost_center_owner,cost_center_owner_email)
                 VALUES(%s,%s,%s,%s,%s) RETURNING id;"""

                    new_cc_id = db_command.add_one_data_sql(db_command.get_postgres_conn(), sql_insert, params)

                    new_ccList.append([new_cc_id, cc_name])

            except Exception as ex:
                error_message = str(ex)
                print(error_message)
                add_error_to_database(12, error_message, tran_id)
                raise ex

        def collect_new_costcenter_to_sent_mail():
            # 'ID', 'CostCenterName'
            try:
                link_all_cc = get_config_value(all_cost_center_key, tran_id)
                link_update_cc = get_config_value(update_cost_center_key, tran_id)

                content_data_dict = {
                    "ContentTitle": f"List New Cost Center From {process_name}",
                    "New_Cost_Center": df_new_cc,
                    "Len_Cols_New_Cost_Center": len(df_new_cc.columns),
                    'LinkAll': link_all_cc,
                    'LinkEach': link_update_cc
                }
                ok = x_mail.send_email(email_type='new_cost_center', transaction_id=tran_id,
                                       attached_file_path=None, content_data_dict=content_data_dict)
                return ok

            except Exception as ex:
                print(str(ex))
                raise ex


        # print(cost_centerList)
        for item in cost_centerList:
            insert_new_cost_center(item)

        if len(new_ccList) > 0:
            df_new_cc = pd.DataFrame(new_ccList, columns=['ID', 'CostCenterName'])
            collect_new_costcenter_to_sent_mail()
             
   except Exception as ex:
        raise ex


   return df_new_cc


def extract_single_file_as_specified_name_in_folder(master_file_path, tran_id):
    print(f"Master File must be {master_file_path}")
    try:
        if os.path.exists(master_file_path) == False:

            path, file = os.path.split(master_file_path)
            filename, extype = os.path.splitext(file)

            print("Main Directory: ", path)
            if os.path.exists(path):

                items = os.listdir(path)
                no_items = len(items)
                print(f"No.file or folder in {path} = {no_items} files")
                if no_items == 1:

                    current_item = os.path.join(path, items[0])
                    print(current_item)

                    if os.path.isfile(current_item):

                        cuurent_name, currnet_extype = os.path.splitext(items[0])
                        if extype == currnet_extype:
                            fd_mn.rename_file(current_item, master_file_path)
                        else:
                            raise Exception(
                                f"{items[0]} cannot rename file due to not being the same file type as  {extype}")

                    else:
                        raise Exception(f"{current_item} is not file.")


                elif no_items > 1:
                    print(items)
                    raise Exception(f"allowed {path} to contain ony one source file.")
                else:
                    raise Exception(f"not found any files in {path}.")

            else:
                raise Exception(f"{path} doestn't exist.")


    except Exception as ex:

        error_message = str(ex)
        print(error_message)
        add_error_to_database(27, error_message, tran_id)
        raise ex

    return True

import chargeback_rpt.vm_data_validator as vx
def convert_str_to_date(str_date,required_datetime_format):
    try:
        str_date=str(str_date)
        x_date = datetime.datetime.strptime(str_date, required_datetime_format)
        return x_date

    except Exception as ex:
        ex=Exception(f'{str_date} is invalid datetime as required format. {str(ex)}')
        add_error_to_database(26, str(ex))
        print(str(ex))
        raise ex







def split_comment_to_each_value(value, key):
    "Split string seperated from commma(,) to be cost_center,system_name,created_date"
    # print(key,'-',value)
    n_data = 3
    if vx.isnan(value) == False:

        value_split = value.split(',')

        if (len(value_split) == n_data):

            emptyList = [x for x in value_split if not (x and not x.isspace())]

            if (len(emptyList) == 0):

                if (key == 'cost_center'):
                    return value_split[0]
                elif (key == 'system_name'):
                    return value_split[1]
                elif (key == 'created_date'):
                    return value_split[2]

            else:
                return np.nan

        else:
            return np.nan  # my return error
    else:
        return np.nan




def get_no_days_in_month_for_costing(int_year, int_month,t_id):
    try:
        config_no_month_str = get_config_value("no_days_to_cost", t_id)
        print(f"No.of day in month to calculate = {config_no_month_str} ")
        config_no_month = int( config_no_month_str)

        if config_no_month > 0:
            print(f"{config_no_month} :No.day in month is determined by fixed-value")
            return config_no_month

        elif config_no_month == 0:
            print(f"{config_no_month} :No.day in month is determined by the last day in month")
            return calendar.monthrange(int_year, int_month)[1]

        else:
            ex = Exception("No.days in one month must be positive numberic value")
            raise ex

    except Exception as ex:

        str_x = f'Error in key=no_days_to_cost in table config_value={config_no_month} :{str(ex)}'
        ex = Exception(str_x)

        add_error_to_database(28, str(ex), t_id)
        raise ex



