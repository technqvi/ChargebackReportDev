# Version 2  adding HPE Storage

import pandas as pd
import numpy as np

import chargeback_rpt.vm_data_utility as vm_util

import chargeback_rpt.vm_data_charger as vc
import chargeback_rpt.db_postgres_command as db_command
import datetime  
import chargeback_rpt.email_notifier as x_mail

import calendar





def create_costing_detail(month_param,year_param):
    test_mid_cost=True
    test_file_detail_report=r'D:\ChargeBackApp\temp'


    t_id=vm_util.creating_transaction(3,month_param,year_param)
    print(f"ETL Transaction ID: {t_id}")

    list_error=[]
    print(list_error)


    def check_error_point(tran_id):
      print(list_error)
      if True in list_error:


        vm_util.collect_error_to_sent_mail(tran_id)

        print("Create detail data occured error")

        raise Exception("Program is teminated and check error from email and log_error.txt")

      list_error.clear()
      print(list_error)


    print("Get no. month to calculate interim-month")

    try:

        no_day_in_month=vm_util.get_no_days_in_month_for_costing(int(year_param),int(month_param),t_id)
        process_cost_date=datetime.datetime(int(year_param),int(month_param),int(no_day_in_month))

    except:
        list_error.append(True)

    check_error_point(t_id)
    print(f'{month_param}-{year_param} apply {no_day_in_month} days to process cost at {process_cost_date}' )

    table_vm="report_vm_info"
    xcol_vm="vm"


    xcol_date='created_date'
    xcol_terminated_date='terminated_date'

    x_col_state_vm='powerstate'
    state_vm='poweredOn'


    notDisplayCols_vm=['id','transaction_id', 'primary_ip_address','additional_info','vm_id']
    notDisplayCols_storge=['id','transaction_id', 'comment']


    table_st="report_storage_info"
    xcol_st="volume_name"

    vm_costtype_params=['vm_cost','backup_cost']
    storage_costtype_params=['storage_cost']


    out_Cols=[]
    in_Cols=[]


    def rename_display_columns(dfx):

     report_cols=dfx.columns.tolist()
     process_cols=[]
     display_cols=[]
     print(report_cols)
     for col in report_cols:
       sr_field= vc.get_datafield_by_column_name(col)
       if sr_field is not None:
         process_cols.append(col)
         display_cols.append(sr_field['column_display_name'])

     #print(len(process_cols))
     #print(len(display_cols))
     zip_cols=dict(zip(process_cols,display_cols))
     #print(zip_cols)
     dfx.rename(columns=zip_cols,inplace=True)
     return dfx


    # select only columns in datarframe
    def list_calculation_columns(df_cols,cost_items):
      selected_items=[ item  for item in cost_items if item in df_cols ]
      return   selected_items

    def load_dataframe(xtb,xcol,notDisplayCols,xmonth,xyear):

     dfx_info=vc.list_x_info(xtb,xcol,xmonth,xyear)
     if  dfx_info is not None:

      dfx_info=dfx_info[ [col for col in dfx_info.columns.tolist() if col not in notDisplayCols ]    ]

      dfx_info= dfx_info.where(pd.notnull(dfx_info), None)

      if 'import_date' in dfx_info.columns:
       dfx_info['import_date']=dfx_info['import_date'].dt.tz_localize(None)

      if xcol_date in dfx_info.columns:
       dfx_info[xcol_date]=pd.to_datetime(dfx_info[xcol_date],format='%Y-%m-%d')


      print(dfx_info.info())


      columnx_info=dfx_info.columns.tolist()
      print(columnx_info)
      return  dfx_info,columnx_info
     else:
      return None,None



    df_vm,all_column_vm=load_dataframe(table_vm,xcol_vm,notDisplayCols_vm,month_param,year_param)
    if df_vm is None:
      list_error.append(True)
      error_message=f"not found data in {table_vm} table on condition month={month_param} and year={year_param}"
      print(error_message)
      vm_util.add_error_to_database(17,error_message,t_id)



    check_error_point(t_id)



    df_category_additionalcost= vc.get_category_for_additional_cost()
    print(df_category_additionalcost)

    print("select column addtional cost from dataframe for calculation")
    cate_master=list_calculation_columns(all_column_vm,df_category_additionalcost['column_table_name'].tolist())
    print(cate_master)


    print("***************add addition(software) column********************")
    in_Cols.extend(cate_master)
    print(in_Cols)


    dict_cate_master={}
    for cate in cate_master:

      dict_cate_master[cate]=df_vm[cate].unique()

    print(dict_cate_master)


    # df_vmOff=df_vm.query("powerstate!=@state_vm and only_backup==False")
    # df_vmOn=df_vm.query("powerstate==@state_vm and only_backup==False")


    df_vmOn=df_vm.query("only_backup==False")
    df_vmOff=pd.DataFrame(columns =df_vm.columns.tolist())

    df_onlyBackup=df_vm.query("only_backup==True")

    if df_vm.shape[0]!=df_vmOff.shape[0]+df_vmOn.shape[0]+df_onlyBackup.shape[0] :
     list_error.append(True)
     error_message= f'some data in VmIfo are wrong with powerstate and only_backup column'
     print(error_message)
     vm_util.add_error_to_database(17,error_message,t_id)

    else:
     print('data is correct')

    check_error_point(t_id)

    df_vm=None

    print('1.list vm keep status off')
    print(df_vmOff[['capacity_gb','os','backup_size_gb']])
    print("*************************************************************************")

    print('2.list vm keep status on')
    print(df_vmOn[['vm','cpu','memory','capacity_gb','os','backup_size_gb']])
    print("*************************************************************************")

    print('3.list backpup only but no vm')
    print(df_onlyBackup[['vm','cpu','memory','capacity_gb','os','backup_size_gb']])
    print("*************************************************************************")

    print("create column for master-calculation")
    df_master= vc.get_all_master_cost()
    df_masterOff=df_master.query('only_online==False')

    df_masterBackup=df_master.query("column_table_name=='backup_size_gb'")

    print(df_master[['column_table_name','cost_unit','only_online','column_display_name','cost_column_display_name']])
    print("========================================================================================")
    print(df_masterOff[['column_table_name','cost_unit','only_online','column_display_name','cost_column_display_name']])
    print("========================================================================================")
    print(df_masterBackup[['column_table_name','cost_unit','only_online','column_display_name','cost_column_display_name']])

    print("select column cost from dataframe for calculation")

    x_master=list_calculation_columns(all_column_vm,df_master['column_table_name'].tolist())
    x_off_master=list_calculation_columns(all_column_vm,df_masterOff['column_table_name'].tolist())
    x_backup_master=list_calculation_columns(all_column_vm,df_masterBackup['column_table_name'].tolist())

    print(x_master)
    print(x_off_master)
    print(x_backup_master)


    print("***************add master cost column********************")
    in_Cols.extend(x_master)
    print(in_Cols)

    print("create master-cost column name and set default value")

    # store cost-value column name , they are excluded in mapping colum display name step
    xCostColName=[]
    for x in x_master:
     sr_temp=(df_master.query('column_table_name==@x')).iloc[0]
     xCostColName.append(sr_temp['cost_column_display_name'])


    print(xCostColName)

    df_vmOn[xCostColName]=0.0
    df_vmOff[xCostColName]=0.0
    df_onlyBackup[xCostColName]=0.0

    # print(df_vmOff.info())
    # print(df_vmOn.info())
    # print(df_onlyBackup.info())



    print("***************add master result column********************")
    out_Cols.extend(xCostColName)
    print(out_Cols)


    def process_cal_masterCost(dfx,costList,df_cost):

      for cost_name in costList:

        # find item by column name  and  convert dataframe to series
        #item_cost=(df_cost.query('column_table_name==@cost_name')).iloc[0]
        item_cost=(df_cost[df_cost['column_table_name']==cost_name]).iloc[0]

        costVal_column=item_cost['cost_column_display_name']
        #print(cost_name,' , ',costVal_column)
        #print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")

        x_unit=item_cost['cost_unit']
        # add cost column at right here
        dfx[costVal_column]=dfx[cost_name ].apply(lambda val: val* x_unit)

      return dfx

    df_vmOn=process_cal_masterCost(df_vmOn,x_master,df_master)
    df_vmOff=process_cal_masterCost(df_vmOff,x_off_master,df_masterOff)
    df_onlyBackup=process_cal_masterCost(df_onlyBackup,x_backup_master,df_masterBackup)
    print(df_vmOn[[xcol_vm]+x_master+out_Cols])
    print(df_vmOff[[xcol_vm]+x_master+out_Cols])
    print(df_onlyBackup[[xcol_vm]+x_master+out_Cols])

    print("++++++++++++++++++++Show newly created master-cost columns++++++++++++++++++++++++")
    print(df_vmOn.info())


    print("create column for fixed-calculation")
    df_fixed= vc.get_all_fixed_cost()
    df_fixedOff=df_fixed.query('only_online==False')
    df_fixedฺBackupSW=df_fixed.query("column_table_name=='backup_software'")

    x_fixed=df_fixed['column_table_name'].tolist()
    x_off_fixed=df_fixedOff['column_table_name'].tolist()
    x_backupSW_fixed=df_fixedฺBackupSW['column_table_name'].tolist()

    print(df_fixed[['column_table_name','cost_unit','only_online','column_display_name','cost_column_display_name']])
    print(df_fixedOff[['column_table_name','cost_unit','only_online','column_display_name','cost_column_display_name']])
    print(df_fixedฺBackupSW[['column_table_name','cost_unit','only_online','column_display_name','cost_column_display_name']])
    print("==================================================================================================")
    print("FixCost for VM=ON",x_fixed)
    print("FixCost for VM=OFF",x_off_fixed)
    print("FixCost for no longer existing VM",x_backupSW_fixed)


    print("create fixed-cost column-name")


    # store cost-value column name , they are excluded in mapping colum display name step
    fixedCostColName=[]
    for x in x_fixed:
     sr_temp=(df_fixed.query('column_table_name==@x')).iloc[0]
     fixedCostColName.append(sr_temp['cost_column_display_name'])

    print(fixedCostColName)

    df_vmOn[fixedCostColName]=0.0
    df_vmOff[fixedCostColName]=0.0
    df_onlyBackup[fixedCostColName]=0.0

    # print(df_vmOff.info())
    # print(df_vmOn.info())
    # print(df_onlyBackup.info())


    print("***************add fixe result column********************")
    out_Cols.extend(fixedCostColName)
    print(out_Cols)


    def calFixedCost(row,item_cost):

      if item_cost['ref_column_table_name']!=None:
        x=item_cost['ref_column_table_name']
        x_val=row[x]
        if x_val>0 :
          return item_cost['cost_unit']
        else:
          return 0
      else:
          return item_cost['cost_unit']

    def process_cal_fixedCost(dfx,costList,df_cost):

     for cost_name in costList:

        # find item by column name  and  convert dataframe to series
        #item_cost=(df_cost.query('column_table_name==@cost_name')).iloc[0]
        item_cost=(df_cost[df_cost['column_table_name']==cost_name]).iloc[0]

        costVal_column=item_cost['cost_column_display_name']
         # add fixed cost
        dfx[costVal_column]=dfx.apply(calFixedCost,axis=1,args=(item_cost,))

     return dfx

    df_vmOn=process_cal_fixedCost(df_vmOn,x_fixed,df_fixed)
    df_vmOff=process_cal_fixedCost(df_vmOff,x_off_fixed,df_fixedOff)
    df_onlyBackup=process_cal_fixedCost(df_onlyBackup,x_backupSW_fixed,df_fixedฺBackupSW)

    print(df_vmOn[['vm']+fixedCostColName])
    print(df_vmOff[['vm']+fixedCostColName])
    print(df_onlyBackup[['vm']+fixedCostColName])
    print("++++++++++++++++++++Show further newly created fixed-cost columns++++++++++++++++++++++++")
    print(df_vmOn.info())

    additionalCostColName=[]
    for x in cate_master:
     sr_temp=(df_category_additionalcost.query('column_table_name==@x')).iloc[0]
     additionalCostColName.append(sr_temp['cost_column_display_name'])

    print("create addtional-cost column(software cost) name and set default value")

    print(additionalCostColName)

    df_vmOn[additionalCostColName]=0.0
    df_vmOff[additionalCostColName]=0.0
    df_onlyBackup[additionalCostColName]=0.0

    print(df_vmOff.head())
    print(df_vmOn.head())
    print(df_onlyBackup.head())


    print("***************add addditional result column********************")
    out_Cols.extend(additionalCostColName)
    print(out_Cols)



    df_addt_cost=None
    for cate_key,cost_nameList in dict_cate_master.items():
      if df_addt_cost is None:
        df_addt_cost=vc.list_additional_cost_by_listCostName(cost_nameList )
      else:
        df_temp  =vc.list_additional_cost_by_listCostName(cost_nameList)
        df_addt_cost=pd.concat([df_addt_cost,df_temp],axis=0)

    df_addt_cost= df_addt_cost.where(pd.notnull(df_addt_cost), None)
    print(df_addt_cost[['cost_name','cost_unit','cost_cal_unit','master_cost_unitbase','ref_column_table_name']])
    #print(df_addt_cost[['cost_unit','cost_cal_unit','master_cost_unitbase','ref_column_table_name']])

    print(df_addt_cost.info())

    # calulate only power on
    def calAddtionalCost_ref(item,itemCost):

    #  xyz=0.0
    #  if item['powerstate']!=state_vm : # powerOff
    #   xyz=0.0
    #  else: # powerOn
        # cal Cal server
      if (itemCost['cost_cal_unit'] is not None) and  (itemCost['master_cost_unitbase'] is None)  and (item['database_no_cal'] >0):
         noCal=item['database_no_cal']
         costCal=itemCost['cost_cal_unit']
         xyz=itemCost['cost_unit']+  (noCal*costCal)
        # cal Core server
      elif (itemCost['master_cost_unitbase'] is not None)  and (itemCost['ref_column_table_name'] is not None) and (itemCost['cost_cal_unit'] is None):
         ref_col=itemCost['ref_column_table_name']
         unitbase=itemCost['master_cost_unitbase']
         xyz= itemCost['cost_unit']*(item[ref_col]/unitbase)
        # cal only server
      else :
         xyz=itemCost['cost_unit']


      return xyz


    def calAddtionalCost(item,cate_x):
     #print(cate_x, " - ",item[cate_x])
     x=item[cate_x]

     abc=0.0
     if x is not None:
      itemCost_df=(df_addt_cost.query('cost_name==@x and column_table_name==@cate_x'))
      if len(itemCost_df)==1 :
        itemCost=itemCost_df.iloc[0]
        #print(itemCost['cost_name','column_table_name','cost_column_display_name','cost_unit','master_cost_unitbase','ref_column_table_name','only_online'])
        abc=calAddtionalCost_ref(item,itemCost)
      else:
          # add error to list and database
        list_error.append(True)
        error_message=f"Not found ={cate_x} and cost={x} in master_additional_cost for vm {item['vm']} at {month_param}-{year_param} in {table_vm}"
        print(error_message)
        vm_util.add_error_to_database(18,error_message,t_id)

     #else:  # not calculate
        #print("none")

     return abc

    # dict_cate_master
    # df_vmOn/df_vmOff ,cate_master,df_category_additionalcost,df_addt_cost
    def process_cal_AddtionalCost(dfx,cateList):

     for cate_col in cateList:
          print("List Columns to take them to caculate additional cost")
          cate_item=(df_category_additionalcost.query('column_table_name==@cate_col')).iloc[0]
          cate_name=cate_item["column_table_name"]
          cate_costDispName=cate_item["cost_column_display_name"]
          print(cate_name,"-",cate_costDispName)

          dfx[cate_costDispName]=dfx.apply(calAddtionalCost,axis=1,args=(cate_name,))

     return dfx

    df_vm=pd.concat([df_vmOn,df_vmOff])
    df_vm=process_cal_AddtionalCost(df_vm ,cate_master)

    df_vm=pd.concat([df_vm,df_onlyBackup])

    df_vm.reset_index(drop=True,inplace=True)



    check_error_point(t_id)


    # for index,item in df_master.iterrows():

    def creat_totalColumns_costType(item,costTypeDict_x):
      type_id=item['cost_type_id']
      x_total=item['cost_column_display_name']
      #print(type_id,'-',x_total)

      if  type_id in costTypeDict_x:
       if  costTypeDict_x[type_id] is None:
        costTypeDict_x[type_id]=[x_total]
       else:
        xxx=costTypeDict_x[type_id]
        if x_total not in xxx:
         xxx.append(x_total)
         costTypeDict_x[type_id]=xxx


    def sum_totalColumns_costType(ctDict,dfCType,dfxInfo):
     #print(ctDict)
     for key,listItem in ctDict.items():
        report_name=(dfCType.query('id==@key').iloc[0])['report_name']
        dfxInfo[report_name]=0.0
        #print(report_name)
        #print("================================")
        for item_cost in listItem:
            #print(item_cost)
            dfxInfo[report_name]+=dfxInfo[item_cost]

     return dfxInfo

    def create_total_DF_Dict(listTypeId):
        ct_df=vc.list_cost_type(listTypeId)
        ct_dict=dict.fromkeys(ct_df['id'].tolist())
        return  ct_df,ct_dict

    print("Get Total Summary Cost Type of VM and Backup")
    cost_type_df,costTypeDict=create_total_DF_Dict(vm_costtype_params)

    if len(cost_type_df)!=len(vm_costtype_params):
     list_error.append(True)
     error_message=f"Not found  some type name in table cost type as follows {vm_costtype_params}"
     print(error_message)
     vm_util.add_error_to_database(19,error_message,t_id)
    else:
     print(cost_type_df)
     print(costTypeDict)

     print("***************add total summary result column********************")
     out_Cols.extend(cost_type_df['report_name'].tolist())
     print(out_Cols)


    check_error_point(t_id)

    print(costTypeDict)
    print(df_master[['column_table_name','cost_column_display_name','cost_type_id']])
    total_temp=df_master.apply(creat_totalColumns_costType,axis=1,args=(costTypeDict,))

    print(df_fixed[['column_table_name','cost_column_display_name','cost_type_id']])
    total_temp=df_fixed.apply(creat_totalColumns_costType,axis=1,args=(costTypeDict,))

    print(df_addt_cost[['column_table_name','cost_column_display_name','cost_type_id']])
    total_temp=df_addt_cost.apply(creat_totalColumns_costType,axis=1,args=(costTypeDict,))
    print(costTypeDict)

    df_vm=sum_totalColumns_costType(costTypeDict,cost_type_df,df_vm)
    print(df_vm.info())
    print(df_vm)





    def xyz(df_xxx,xcol_key,cost_Cols, is_vm_cost):
        "Paramters are dataframe,dict_params : col_key as string,col_created_date as date,cost_columns as list,process_cost_date as datetime,no_days_in_month as int"

        try:
            no_month_for_price_day=30
            #no_month_for_price_day=no_day_in_month

            print("2.Split in-month and full-month")


            df_InMonth = df_xxx[(df_xxx[xcol_date].dt.month == process_cost_date.month) & (
                    df_xxx[xcol_date].dt.year == process_cost_date.year)].copy()

            df_FullMonth = df_xxx[~(df_xxx[xcol_key].isin(df_InMonth[xcol_key].tolist()))].copy()

            ex_UsedDay_col = 'ex_UsedDay'
            ex_TotalDay_col = 'ex_NoDayToAverageCost'

            def cal_x(df_temp):
                df_temp[ex_TotalDay_col] = no_month_for_price_day

                for cost_item in cost_Cols:
                    df_temp[cost_item] = (df_temp[cost_item] / df_temp[ex_TotalDay_col]) * df_temp[ex_UsedDay_col]

                print("#############Show No.Used Day For In Month################")
                if  is_vm_cost==True:
                 print(df_temp[[xcol_key,xcol_date,xcol_terminated_date,ex_UsedDay_col,ex_TotalDay_col]])
                else:
                 print(df_temp[[xcol_key,xcol_date,ex_UsedDay_col,ex_TotalDay_col]])
                print("#############-----------------------------################")

                df_temp.drop(columns=[ex_UsedDay_col, ex_TotalDay_col], inplace=True)

                return df_temp

            # In month  you have to have No.Day of Usage
            if df_InMonth.empty == False:

                print(f"3.1#1 {xcol_key} In-Month :No-Rows= {len(df_InMonth)} Before Calculation")
                print(df_InMonth[[xcol_key, xcol_date] + cost_Cols])

                print(f"Calculate {xcol_key} In-month")


                if is_vm_cost==True:
                  print("Cal VM In Month (VM contain terminated_date column)")

                  dfInMonth_Not_Null= df_InMonth[df_InMonth[xcol_terminated_date].notnull()].copy()
                  if dfInMonth_Not_Null.empty == False:

                    print("terminated_date value in Cal VM In Month")
                    dfInMonth_Not_Null[ex_UsedDay_col]=  dfInMonth_Not_Null.apply(lambda item: (item[xcol_terminated_date].day-item[xcol_date].day)+1  ,axis=1)

                  dfInMonth_Null= df_InMonth[df_InMonth[xcol_terminated_date].isnull()].copy()
                  if dfInMonth_Null.empty == False:

                    print("no terminated_date value in Cal VM In Month")
                    dfInMonth_Null[ex_UsedDay_col]=  dfInMonth_Null.apply(lambda item: (no_day_in_month - item[xcol_date].day)+1  ,axis=1)

                  df_InMonth=pd.concat([dfInMonth_Not_Null,dfInMonth_Null])
                  df_InMonth.reset_index(drop=True,inplace=True)


                else:
                  print("Cal Storage In Month (no terminated_date column )" )
                  df_InMonth[ex_UsedDay_col] = df_InMonth[xcol_date].apply(lambda dtx: (no_day_in_month - dtx.day)+1)

                df_InMonth=cal_x(df_InMonth)

                print(f"3.1#2 {xcol_key} In-Month : {len(df_InMonth)} After Calculation")
                print(df_InMonth[[xcol_key, xcol_date] + cost_Cols])


            else:
                print(f"No {xcol_key} In-Month to calculate")


            print("=================================================================================")
            if df_FullMonth.empty == False:

                print(f'3.2#1 {xcol_key} Full-Month : No-Rows= {len(df_FullMonth)} Before Calculation')
                print(df_FullMonth[[xcol_key, xcol_date] + cost_Cols])
                # VM
                if is_vm_cost==True:

                  # vm contain terminated_date certainly
                  print("Full month ==> VM")

                  df_FullMonth_Not_Null= df_FullMonth[df_FullMonth[xcol_terminated_date].notnull()].copy()
                  df_FullMonth_Null= df_FullMonth[df_FullMonth[xcol_terminated_date].isnull()].copy()

                  if  df_FullMonth_Not_Null.empty==False :
                    print("terminated_date value in Cal VM Full Month")
                    df_FullMonth_Not_Null[ex_UsedDay_col] =df_FullMonth_Not_Null.apply(lambda item:item[xcol_terminated_date].day  ,axis=1)
                    df_FullMonth_Not_Null=cal_x(df_FullMonth_Not_Null)

                  df_FullMonth=pd.concat([df_FullMonth_Not_Null,df_FullMonth_Null])
                  df_FullMonth.reset_index(drop=True,inplace=True)

                # storage
                else:
                    print("Full month ==> Storage")

                print(f'3.2#2 {xcol_key} Full-Month : No-Rows={len(df_FullMonth)} After Calculation')
                print(df_FullMonth[[xcol_key, xcol_date] + cost_Cols])

            else:
                print(f"No {xcol_key} Full-Month to calculate")

            print(f"4.Merge {xcol_key} Dataframe")
            df_xxx = pd.concat([df_InMonth, df_FullMonth])
            df_xxx = df_xxx.reset_index(drop=True)

            print(df_xxx[[xcol_key, xcol_date] + cost_Cols])
            #print(df_xxx.info())

            return df_xxx

        except Exception as ex:
            raise  ex


    print(df_vm[[ xcol_vm,x_col_state_vm, xcol_date,xcol_terminated_date] ])
    #df_vm[[ xcol_vm,x_col_state_vm, xcol_date,xcol_terminated_date] + in_Cols+out_Cols ].to_excel(f'{test_file_detail_report}\\vm_before_adjust_cost.xlsx')



    print(f"1.Split poweredOn to calculate")
    dfx_on = df_vm[ (df_vm['powerstate']=='poweredOn')].copy()
    print(dfx_on[[ xcol_vm,x_col_state_vm, xcol_date,xcol_terminated_date] + in_Cols+out_Cols ])

    #dfx_on[[ xcol_vm,x_col_state_vm, xcol_date,xcol_terminated_date] + in_Cols+out_Cols ].to_excel(f'{test_file_detail_report}\\vm_on_before_adjust_cost.xlsx')

    dfx_on=xyz(dfx_on,xcol_vm,out_Cols,True)

    #dfx_on[[ xcol_vm,x_col_state_vm, xcol_date,xcol_terminated_date] + in_Cols+out_Cols ].to_excel(f'{test_file_detail_report}\\vm_on_after_adjust_cost.xlsx')

    print(f"1.Split poweredOff to calculate")
    dfx_off = df_vm[ (df_vm['powerstate']=='poweredOff')].copy()


    print(dfx_off[[ xcol_vm,x_col_state_vm, xcol_date,xcol_terminated_date] + in_Cols+out_Cols ])
    #dfx_off[[ xcol_vm,x_col_state_vm, xcol_date,xcol_terminated_date] + in_Cols+out_Cols ].to_excel(f'{test_file_detail_report}\\vm_off_before_adjust_cost.xlsx')



    dfx_off=xyz(dfx_off,xcol_vm,out_Cols,True)


    #dfx_off[[ xcol_vm,x_col_state_vm, xcol_date,xcol_terminated_date] + in_Cols+out_Cols ].to_excel(f'{test_file_detail_report}\\vm_off_after_adjust_cost.xlsx')


    print("VM Cost Online and Offline")

    df_vm=pd.concat([dfx_on,dfx_off])
    df_vm.reset_index(drop=True,inplace=True)

    #df_vm[[ xcol_vm,x_col_state_vm, xcol_date,xcol_terminated_date] + in_Cols+out_Cols ].to_excel(f'{test_file_detail_report}\\vm_after_adjust_cost.xlsx')


    print(df_vm[[ xcol_vm,x_col_state_vm, xcol_date,xcol_terminated_date] + in_Cols+out_Cols ])



    # for  actual data
    df_vm=rename_display_columns(df_vm)
    print(df_vm)



    def get_storage_for_calculations(table_x,cols_x,notDisplayCols_x):

     print(f"load storge data from {table_x} by month and year")
     df_x,all_column_x=load_dataframe(table_x,cols_x,notDisplayCols_x,month_param,year_param)

     if df_x is not None:
      print(f"select column cost from {table_x}  for calculation")
      # df_master is global variable
      storage_master_cal=list_calculation_columns(all_column_x,df_master['column_table_name'].tolist())
      print(storage_master_cal)

      return  df_x,all_column_x,storage_master_cal
     else:
        return None,None,None

    def cal_cost_value(df_x,storage_master_x):
     print("create storage master-cost column name and set default value")

     # df_master is global variable
     xyz_storageCostColName=[]
     for x in storage_master_x:
       sr_temp=(df_master.query('column_table_name==@x')).iloc[0]
       xyz_storageCostColName.append(sr_temp['cost_column_display_name'])

     print(xyz_storageCostColName)

     df_x[xyz_storageCostColName]=0.0

     print("calculate cost storage")

     df_x=process_cal_masterCost(df_x,storage_master_x,df_master)
     return df_x , xyz_storageCostColName



    def change_cost_column_name(x_costtype_params,xCostColName):

        print("Get Total Summary Cost Type of Storage")
        cost_type_df,costTypeDict=create_total_DF_Dict(x_costtype_params)
        print(cost_type_df,costTypeDict)

        if len(cost_type_df)!=len(x_costtype_params):
         list_error.append(True)
         error_message=f"Not found  some type name in table cost type as follows {x_costtype_params}"
         print(error_message)
         vm_util.add_error_to_database(19,error_message,t_id)
        else:
          new_xCostColName=cost_type_df['report_name'].tolist()
          print("New Cost Name ",new_xCostColName)


        if  len(new_xCostColName)!=len(xCostColName):
         list_error.append(True)
         error_message=f"Not allow storage-cost more than one in  either datafield_mapping or cost_type  table"
         print(error_message)
         vm_util.add_error_to_database(19,error_message,t_id)
         return None
        else:
          print("Mapping name To New Cost Column Name")
          xyz_cost_x_map=dict(zip(xCostColName,new_xCostColName) )
          print(xyz_cost_x_map)
          return xyz_cost_x_map

    print("Calcualate cost NetApp")
    df_storage,all_column_storage,storage_master=get_storage_for_calculations(table_st,xcol_st,notDisplayCols_storge)

    print("1.Get Data for NetApp Cost")

    if df_storage  is not None:
        print("=======================================================")

        print(all_column_storage)
        print(storage_master)

        print("=======================================================")
        print("2.Calculate data as full month")
        df_storage,storageCostColName=cal_cost_value(df_storage,storage_master)
        print(storageCostColName)
        print(df_storage)

        check_error_point(t_id)

        print("=======================================================")
        print("3.Adjust Cost  Storage as intra-month")

        df_storage=xyz(df_storage,xcol_st,storageCostColName,False)
        check_error_point(t_id)

        print("4.Change cost column as cost_type table")
        storage_cost_x_map=change_cost_column_name(storage_costtype_params,storageCostColName)

        df_storage=rename_display_columns(df_storage)
        df_storage=df_storage.rename(columns=storage_cost_x_map)

        check_error_point(t_id)

        print(df_storage)
    else:
        print(f"No data for netapp in {month_param}-{year_param}")



    print("Calcualate cost Nimble")
    nimble_table="report_nimble_info"
    nimble_xcol="volume_name_nimble"
    nimble_notDisp=['id','transaction_id', 'comment_nimble']
    nimble_costtype_params=['nimble_cost']

    df_nimble,all_column_nimble,nimble_master=get_storage_for_calculations(nimble_table,nimble_xcol,nimble_notDisp)

    print("1.Get Data for Nimble Cost")

    if df_nimble  is not None:

        print("=======================================================")

        print(all_column_nimble)
        print(nimble_master)

        print("=======================================================")
        print("2.Calculate data nimble")
        df_nimble,nimbleCostColName=cal_cost_value(df_nimble,nimble_master)
        print(df_nimble)
        print(nimbleCostColName)
        check_error_point(t_id)

        print("3.Nimble Adjust Cost  Storage as intra-month")

        df_nimble=xyz(df_nimble,nimble_xcol,nimbleCostColName,False)


        check_error_point(t_id)

        print("4.Change cost column")
        nimble_cost_x_map=change_cost_column_name(nimble_costtype_params,nimbleCostColName)

        df_nimble=rename_display_columns(df_nimble)
        df_nimble=df_nimble.rename(columns=nimble_cost_x_map)

        check_error_point(t_id)

        print(df_nimble)
    else:
        print(f"No data for nimble in {month_param}-{year_param}")

    print("Calcualate cost Primera")
    primera_table="report_primera_info"
    primera_xcol="volume_name_primera"
    primera_notDisp=['id','transaction_id', 'comment_primera']
    primera_costtype_params=['primera_cost']

    df_primera,all_column_primera,primera_master=get_storage_for_calculations(primera_table,primera_xcol,primera_notDisp)

    print("1.Get Data for Primera Cost")

    if df_primera  is not None:

        print("=======================================================")

        print(all_column_primera)
        print(primera_master)


        print("=======================================================")
        print("2.Calculate data primera")
        df_primera,primeraCostColName=cal_cost_value(df_primera,primera_master)
        print(primeraCostColName)
        print(df_primera)
        check_error_point(t_id)

        print("3.Primera Adjust Cost  Storage as intra-month")

        df_primera=xyz(df_primera,primera_xcol,primeraCostColName,False)
        check_error_point(t_id)

        print("4.Change cost column")
        primera_cost_x_map=change_cost_column_name(primera_costtype_params,primeraCostColName)

        df_primera=rename_display_columns(df_primera)
        df_primera=df_primera.rename(columns=primera_cost_x_map)

        check_error_point(t_id)

        print(df_primera)
    else:
        print(f"No data for primera in {month_param}-{year_param}")


    ref_master=df_master[['description','cost_unit']].copy()
    ref_master=ref_master.rename(columns={'column_table_name': 'Cost','cost_unit': 'Price/Unit(Bath)'},)
    print(ref_master)

    ref_fixed=df_fixed[['description','cost_unit','ref_column_table_name']].copy()
    #df = df.fillna("")
    ref_fixed['ref_column_table_name']=ref_fixed['ref_column_table_name'].apply(lambda item: ' ' if item is None else item )

    ref_fixed=ref_fixed.rename(columns={'column_table_name': 'Cost','cost_unit': 'Price/Unit(Bath)',
                                       'ref_column_table_name':'Ref-Other'
                                       })
    print(ref_fixed)

    ref_addtion=df_addt_cost[['cost_name','cost_unit','cost_cal_unit','ref_column_table_name'
                              ,'master_cost_unitbase','column_table_name']].copy()
    none_to_emp_cols=['ref_column_table_name','master_cost_unitbase','cost_cal_unit','cost_cal_unit']

    #df = df.fillna("")
    for xxx in none_to_emp_cols:
      ref_addtion[xxx]= ref_addtion[xxx].apply(lambda item: ' ' if item is None else item )


    ref_addtion=ref_addtion.rename(columns={'cost_name': 'Cost',
                                        'cost_unit': 'Price/Unit(Bath)','cost_cal_unit':'CAL Price/Unit(Bath)',
                                       'ref_column_table_name':'Ref-Other','master_cost_unitbase':'Core UnitBase',
                                        'column_table_name':'Category'
                                       })

    print(ref_addtion)

    try:

        updated_rows=vm_util.created_transaction(t_id)
        print("completed building detail data")

    except Exception as ex:
        list_error.append(True)
        print(ex)

    check_error_point(t_id)

    data_dict={'vm_detail':df_vm,
              'storage_detail':df_storage,
               'nimble_detail':df_nimble,
               'primera_detail':df_primera,
               'cost_ref_master':ref_master,
              'cost_ref_fixed':ref_fixed,
              'cost_ref_addition':ref_addtion
             }

    print("================Thank you for your effort==============")
    print("Completed cost calculation")

    return data_dict





