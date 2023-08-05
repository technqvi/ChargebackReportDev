import chargeback_rpt.db_postgres_command as db_command
import chargeback_rpt.veeam_data_extractor as rpt_veeam
import chargeback_rpt.vm_data_utility as vm_util
import chargeback_rpt.vm_data_charger as vc

import pandas as pd
import numpy as np

import os

import datetime
import json
import math
import re
def isnan(value):
  try:
      import math
      return math.isnan(float(value))
  except:
      return False

# def replace_x_character_old(value,currentCharList,newChar):
#     new_value=value
#     # Alternatively ,REPLACE ALL Chart(include newChar) TO ' ' and replace  ' ' with newChar
#
#     for old in curr
#     entCharList:
#        new_value = re.sub('\s+', old, new_value)
#        if old in new_value:
#         new_value=new_value.replace(old,newChar)
#
#     print("old ",value , " vs new ",new_value)
#     return new_value
#change method
def replace_x_character(value):
#check  if found special_char  or white space , report error
 try:

  if isnan(value):
      value=None

  if value is not None:
    new_value=str(value)
    new_value=new_value.strip()
    # load from database
    special_char=vm_util.get_value_by_key('special_char')
    special_char=special_char['value']
    special_char=special_char.split(',')
    special_char=[ x.strip() for x in  special_char]

    preferable_char=vm_util.get_value_by_key('preferable_char')['value']

    special_char.append(preferable_char)

    #new_value=[  new_value.replace(sc,' ') if sc in new_value  ]
    for sc in special_char:
        if sc in new_value:
            new_value = new_value.replace(sc, ' ')

    new_value = re.sub('\s+', ' ', new_value)
    new_value =new_value.replace(' ',preferable_char)
    #print(new_value)
    return new_value
 except Exception as error:
     raise error
 else:
     return None




# error type refer error_type table
def check_source_name_file(source_name,tran_id):
    err=False;
    sr=vm_util.get_sourceFile_info(source_name)
    if sr is None:
        error_message= f'{source_name} was not found in source name column in  datasource_mapping.'
        vm_util.add_error_to_database(4,error_message,tran_id)
        print(error_message)
        err=True
    return sr,err


def check_existing_filepath(path, tran_id):
    error_message = None
    isExisting = os.path.exists(path)
    if isExisting == True:
        return True, False
    else:
        error_message = f'{path} was not found'
        vm_util.add_error_to_database(2, error_message, tran_id)
        print(error_message)

        return False, True

def load_source_file(path,sheet_name,tran_id):
 # 'one sheets and return as dataframe'
 if sheet_name is None:
   sheet_name='Sheet1'
 try:
   dfx=pd.read_excel(path,sheet_name=sheet_name)
   return dfx,False
 except Exception as error:

   error_message= f'{path} cannot load , {error}'
   vm_util.add_error_to_database(2,error_message,tran_id)
   print(error_message)
   return None,True

def load_source_file_as_dict(path, sheet_name_list, skiprows_list,tran_id):
     # 'multiple sheets and return as dictionary so use key to access dataframe'
     try:
         if (sheet_name_list is not None) and len(sheet_name_list)>0:
           if  (skiprows_list is not None) and len(skiprows_list)>0 :
            dict_dfs = pd.read_excel(path, sheet_name=sheet_name_list,skiprows=skiprows_list)
           else:
            dict_dfs = pd.read_excel(path, sheet_name=sheet_name_list)
           return dict_dfs, False
     except Exception as error:
         error_message = f'{path} cannot load , {error}'
         vm_util.add_error_to_database(2, error_message, tran_id)
         print(error_message)
         return None, True


def select_colunm_df_fieldname_ds(df, field_source_name_list, path, tran_id):
    try:
        df = df[field_source_name_list]
        return df, False
    except Exception as error:
        error_message = f'some field of these {field_source_name_list} were not found in {path} , {error}'
        vm_util.add_error_to_database(5, error_message, tran_id)
        print(error_message)
        return None, True
#https://thispointer.com/pandas-find-duplicate-rows-in-a-dataframe-based-on-all-or-selected-columns-using-dataframe-duplicated-in-python/
def find_SameVMName(df,column_name):
    try:
     df_dup=df[df.duplicated(column_name,keep=False)]
     if df_dup.shape[0]>1:
      return df_dup,True
     else:
      return None,False
    except Exception as error:
       print(error)


def find_NaN(df, not_NaN_cols):
    df_na = df[not_NaN_cols]

    # find total NaN
    cols_na = df_na.isna().sum()

    # list rown NaN
    is_NaN = df_na.isnull()
    row_has_NaN = is_NaN.any(axis=1)

    rows_with_NaN = df[row_has_NaN]

    return cols_na, rows_with_NaN



def find_NonNumeric(df):
    numbericCols = vc.list_master_cost()
    numColsList = numbericCols['column_table_name'].tolist()
    nonNumeric_cols = [x for x in df.columns.tolist() if x in numColsList]

    df[nonNumeric_cols] = df[nonNumeric_cols].apply(pd.to_numeric, errors='coerce')

    srNN, dfNN = find_NaN(df, nonNumeric_cols)

    return srNN, dfNN, nonNumeric_cols
def report_ErrorValues(sr_NaN, df_NaN, tran_id,error_type_string,error_type_id):

    str_error = f'{error_type_string} Values Error</br>'

    sr_NaN = sr_NaN[sr_NaN > 0]
    if len(sr_NaN) > 0:
        str_error += f'</br>Summarize {error_type_string} columns</br>'
        str_error += sr_NaN.to_frame(name='Null-Value').to_html()

        str_error += f'</br>Summarize {error_type_string} rows</br>'
        df_NaN= df_NaN.fillna("")
        str_error += df_NaN.to_html(index=False)

        vm_util.add_error_to_database(error_type_id, str_error, tran_id)
        #print(str_error)
        return str_error, True

    else:
        return str_error, False

def map_fields_ds_to_cols_df(df_xyz, x_mappingFields, path, tran_id):
    try:
        source_cols_vInfo = df_xyz.columns.tolist()
        target_cols_vInfo = x_mappingFields['field_source_name'].tolist()
        check_columns = vm_util.verify_existing_columns(source_cols_vInfo, target_cols_vInfo)

        if check_columns is None:
            dict_mapplingCols = dict(zip(x_mappingFields['field_source_name'], x_mappingFields['column_table_name']))
            print(dict_mapplingCols)
            df_xyz.rename(columns=dict_mapplingCols, inplace=True)
            return df_xyz, False
        else:
            raise Exception(check_columns)
    except Exception as error:

        error_message = f'cannot map some fields between file {path}  and dataframe, {error}'
        vm_util.add_error_to_database(7, error_message, tran_id)
        print(error_message)
        return None, True


def map_fields_ds_to_cols_df(df_xyz, x_mappingFields, path, tran_id):
    try:
        source_cols_vInfo = df_xyz.columns.tolist()
        target_cols_vInfo = x_mappingFields['field_source_name'].tolist()
        check_columns = vm_util.verify_existing_columns(source_cols_vInfo, target_cols_vInfo)

        if check_columns is None:
            dict_mapplingCols = dict(zip(x_mappingFields['field_source_name'], x_mappingFields['column_table_name']))
            print(dict_mapplingCols)
            df=df_xyz.rename(columns=dict_mapplingCols)
            return df, False
        else:
            raise Exception(check_columns)
    except Exception as error:

        error_message = f'cannot map some fields between file {path}  and dataframe, {error}'
        vm_util.add_error_to_database(7, error_message, tran_id)
        print(error_message)
        return None, True


def listCols_report_table(tb_key, tran_id):
    tb_sr = vm_util.get_value_by_key(tb_key)

    err = False
    df_table = None
    listCols_report = None
    tb_name = None

    if tb_sr is not None:

        tb_name = tb_sr['value']
        column_name = f'columns_{tb_name}'
        df_table = vm_util.get_xtable(tb_name, column_name)

        if df_table is None:
            err = True
            error_message = f'no table {tb_name} in database'
            print(error_message)
            vm_util.add_error_to_database(9, error_message, tran_id)
        else:
            listCols_report = df_table[column_name].tolist()

    else:
        err = True
        error_message = f'no key:{tb_key} as table name'
        print(error_message)
        vm_util.add_error_to_database(3, error_message, tran_id)

    return tb_name, listCols_report, err

#https://www.tutorialspoint.com/Extracting-email-addresses-using-regular-expressions-in-Python
def validate_string_email(str_x):
    try:
        #regex = '^[a-z0-9]+[\._]?[a-z0-9]+[@]\w+[.]\w{2,3}$'
        regex = "[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+"
        list_invalid_email = []
        # print(str_x)
        str_x = str_x.replace(' ', ',')
        # print(str_rc)
        list_x = str_x.split(',')
        print(list_x)

        for x in list_x:
            if (re.search(regex, x)):
                print(f"{x} is Valid Email")
            else:
                list_invalid_email.append(x)
                print(f"{x} is  Invalid Email")

        if len(list_invalid_email) > 0:
            raise Exception(f'The follwing are invalid email format : {list_invalid_email}')

    except  Exception as ex:
        raise ex

    return True



def find_incorrrect_format_for_input_datetime(df, dateTime_col, key_col, datetime_format, t_id):
    list_error = []

    def convert_string_to_datetime(row):
        try:
            str_date = row[dateTime_col]
            str_date = str(str_date)
            try:
                x_date = datetime.datetime.strptime(str_date, datetime_format)
            except Exception as ex:
                # special error for only a few case -
                if "-" in str_date:
                    str_date = str_date.replace("\u2013", "-")
                    x_date = datetime.datetime.strptime(str_date, datetime_format)

            return x_date



        except Exception as ex:
            # print(ex)
            str_ex = f'{key_col} : {row[key_col]} found error ,Input string on  {dateTime_col} column of {str_date} does not match format {datetime_format}'
            ex = Exception(str_ex)

            vm_util.add_error_to_database(26, str(ex), t_id)
            list_error.append(True)
            print(str(ex))

    df[dateTime_col] = df.apply(convert_string_to_datetime, axis=1)

    if len(list_error) > 0:
        print(list_error)
        return None
    else:

        return df




def find_invalid_x_date_greater_last_date_for_input_datetime(df, dateTime_col,lastDay_val, key_col, t_id):
    list_error = []
    def check_valid_datetime(row):
        try:
            x_date=row[dateTime_col]
            if x_date> lastDay_val:
                str_ex = f'{key_col} : {row[key_col]} found error ,{x_date} can not be more than {lastDay_val}'
                raise Exception(str_ex)
            else:
                return x_date

        except Exception as ex:
            vm_util.add_error_to_database(29, str(ex), t_id)
            list_error.append(True)
            print(str(ex))

    df[dateTime_col] = df.apply(check_valid_datetime, axis=1)

    if len(list_error) > 0:
        print(list_error)
        return None
    else:
        return df
