import chargeback_rpt.db_postgres_command as db_command
import chargeback_rpt.vm_data_utility as vm_util
import json
import pandas as pd

import datetime

import os
import sys

def get_last_complete_job(month,year,job_type_id):
 try:

     sql_cmd = """
   select  * from job_transaction where id in(
         select distinct on (type_id) id from job_transaction where month=%(x_month)s and year=%(x_year)s 
         and type_id=%(x_type)s and completed=True
         order by type_id, start_datetime desc)

     """

     xParams ={"x_month":month,"x_year": year,'x_type':job_type_id}

     item=db_command.get_one_sql(db_command.get_postgres_conn(),sql_cmd,xParams)
     if item is not None:
         sr = pd.Series(item)
         return sr
     else:
         return None

 except Exception as e:
     raise  e



def check_existing_one_info(table_name,month,year):
 try:

     sql_cmd =f"SELECT COUNT(*) FROM   (SELECT 1 FROM   {table_name} WHERE month=%(x_month)s and year=%(x_year)s  LIMIT  1)  one_row";
     xParams ={"x_month":month,"x_year": year}
     item=db_command.get_one_sql(db_command.get_postgres_conn(),sql_cmd,xParams)

     if item is not None:
        if item['count']>=1:
         return True


     return False
 except Exception as e:
     raise  e


def list_x_info(table_name,x_col_name,month,year):
 try:
     # sql_cmd = f'select  * from {table_name} where id in( \
     #     select distinct on ({x_col_name}) id from {table_name} where month=(%s) and year=(%s) order by {x_col_name}, import_date desc)'
     # xParams = (year, month)

     sql_cmd = f'select  * from {table_name} where id in( \
         select distinct on ({x_col_name}) id from {table_name} where month=%(x_month)s and year=%(x_year)s order by {x_col_name}, import_date desc)'

     xParams ={"x_month":month,"x_year": year}

     vinfoTuple = db_command.get_list_sql(db_command.get_postgres_conn(), sql_cmd, xParams)
     if vinfoTuple is not None:
         df = pd.DataFrame(data=vinfoTuple )
         return df
     else:
         return None

 except Exception as e:
     raise  e


def get_x_info(table_name, x_col_name, x_key_val):
    try:
        sql_cmd = f'select  * from {table_name} where id in( \
            select distinct on ({x_col_name}) id from {table_name} where {x_col_name}=(%s)  order by {x_col_name}, import_date desc)'
        xParams = (x_key_val,)


        a_vinfo = db_command.get_one_sql(db_command.get_postgres_conn(), sql_cmd, xParams)
        if a_vinfo is not None:
            sr = pd.Series(data=a_vinfo)
            return sr
        else:
            return None

    except Exception as e:
        raise e


def list_master_cost():
 try:

   sql_cmd="select m_cost.id, f_map.column_table_name ,f_map.field_source_name as field_name_master,m_cost.cost_unit as cost_bath,is_used as is_used " \
             "from master_cost m_cost inner join datafield_mapping f_map on m_cost.cost_column_id=f_map.id  where m_cost.is_used=true"

   sql_params=None
   # if cost_type_id is not None:
   #     sql_cmd = f" {sql_cmd} and cost_column_id=(%s)"
   #     sql_params = (cost_type_id,)

   list_field=db_command.get_list_sql(db_command.get_postgres_conn(),sql_cmd,sql_params)

   if list_field is  not None:
     df=pd.DataFrame(data=list_field)
     return df
   else:
     return None

 except Exception as errors :
     # raise  error
     return None


def list_cost_type(listCostType=None):
    "Takes type_name  as list such as storage_cost,backup_cost,vm_cost"
    sql = """
    select * from cost_type
 """
    if listCostType is None:
        list_cols = db_command.get_list_sql(db_command.get_postgres_conn(), sql, None)
    else:
        #sql = f'{sql} where id IN %s '
        sql = f'{sql} where type_name IN %s '

        list_cols = db_command.get_list_sql(db_command.get_postgres_conn(), sql, (tuple(listCostType),))

    if list_cols is not None:
        df = pd.DataFrame(data=list_cols)
        return df
    else:
        return None


def get_cost_type(costType):
    "Takes type_name  such as storage_cost,backup_cost,vm_cost"
    sql = """
    select * from cost_type where type_name=%s
 """

    item = db_command.get_one_sql(db_command.get_postgres_conn(), sql, (costType,))

    if item is not None:
        sr = pd.Series(data=item)
        return sr
    else:
        return None



def get_datafield_by_column_name(column_name):
 try:
    # focusing on column_table_name,field_source_name
    sql_cmd="select * from datafield_mapping where column_table_name =(%s)"
    sql_params=(column_name,)
    item=db_command.get_one_sql(db_command.get_postgres_conn(),sql_cmd,sql_params)
    if item is not None:
     sr_fieldInfo=pd.Series(item)
     return sr_fieldInfo
    else:
        return None
 except Exception as error:
     #raise  error
     return None


def get_category_for_additional_cost():
 sql="""
   select column_table_name,column_display_name,cost_column_display_name
   from datafield_mapping where id in( select distinct on (cost_column_id) cost_column_id from master_additional_cost UNION SELECT id from datafield_mapping
   where is_additional=True and is_active=True)

 """
 list_cols=db_command.get_list_sql(db_command.get_postgres_conn(),sql,None)

 if list_cols is  not None:
     df=pd.DataFrame(data=list_cols)
     return df
 else:
     return None


def get_all_master_cost():
    sql = """
    select m_cost.id, f_map.column_table_name,f_map.column_display_name,f_map.cost_column_display_name
    ,m_cost.cost_unit ,is_used ,only_online ,cost_type_id,m_cost.description
    from master_cost m_cost inner join datafield_mapping f_map on m_cost.cost_column_id=f_map.id 
    where m_cost.is_used=true
 """
    list_cols = db_command.get_list_sql(db_command.get_postgres_conn(), sql, None)

    if list_cols is not None:
        df = pd.DataFrame(data=list_cols)
        return df
    else:
        return None

def get_all_fixed_cost():
 sql="""
select m_cost.id,  f_map.column_table_name ,f_map.field_source_name 
      ,f_map.column_display_name,f_map.cost_column_display_name
      ,m_cost.cost_unit
     ,f_map2.column_table_name as ref_column_table_name,m_cost.is_used,m_cost.only_online,m_cost.cost_type_id
     ,m_cost.description
from master_fixed_cost m_cost
left join datafield_mapping f_map on m_cost.cost_column_id=f_map.id
left join  master_cost x on m_cost.master_cost_ref_id=x.id
left join datafield_mapping f_map2 on x.cost_column_id=f_map2.id
where  m_cost.is_used=true
 """
 list_cols=db_command.get_list_sql(db_command.get_postgres_conn(),sql,None)

 if list_cols is  not None:
     df=pd.DataFrame(data=list_cols)
     return df
 else:
     return None


def list_additional_cost_by_listCostName(list_costName):
    tuple_costName = tuple(list_costName)

    sql = """

select add_cost.id, add_cost.cost_name ,f_map.column_table_name
     ,f_map.column_display_name,f_map.cost_column_display_name
     ,add_cost.cost_unit ,add_cost.cost_cal_unit
     , add_cost.master_cost_unitbase,f_map2.column_table_name as ref_column_table_name
     , add_cost.is_used,add_cost.only_online,add_cost.cost_type_id,add_cost.description
from master_additional_cost add_cost
inner join datafield_mapping f_map on add_cost.cost_column_id=f_map.id
left join master_cost m_cost on add_cost.master_cost_ref_id=m_cost.id
left join datafield_mapping f_map2 on m_cost.cost_column_id=f_map2.id
where add_cost.cost_name IN %s 
and add_cost.is_used=true
order by f_map.column_table_name ;

 """

    # where add_cost.cost_name =%s and f_map.column_table_name=%s
    sql_param = (tuple_costName,)
    list_cols = db_command.get_list_sql(db_command.get_postgres_conn(), sql, sql_param)

    if list_cols is not None:
        df = pd.DataFrame(data=list_cols)
        return df
    else:
        return None


def get_additional_cost_by_cateName_and_costName(cost_name, cate_name):
    sql = """

select add_cost.id, add_cost.cost_name ,f_map.column_table_name
     ,f_map.column_display_name,f_map.cost_column_display_name
     ,add_cost.cost_unit ,add_cost.cost_cal_unit
     , add_cost.master_cost_unitbase,f_map2.column_table_name as ref_column_table_name
     , add_cost.is_used,add_cost.only_online,add_cost.cost_type_id,add_cost.description
from master_additional_cost add_cost
inner join datafield_mapping f_map on add_cost.cost_column_id=f_map.id
left join master_cost m_cost on add_cost.master_cost_ref_id=m_cost.id
left join datafield_mapping f_map2 on m_cost.cost_column_id=f_map2.id
where add_cost.cost_name =%s and f_map.column_table_name=%s 
and add_cost.is_used=true
order by f_map.column_table_name ;

 """

    sql_param = (cost_name, cate_name,)
    list_cols = db_command.get_list_sql(db_command.get_postgres_conn(), sql, sql_param)

    if list_cols is not None:
        df = pd.DataFrame(data=list_cols)
        return df
    else:
        return None



