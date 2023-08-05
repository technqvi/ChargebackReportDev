import pandas as pd
import numpy as np

import chargeback_rpt.vm_data_utility as vm_util
import chargeback_rpt.db_postgres_command as db_command
import chargeback_rpt.vm_data_charger as vc

import os
import datetime
import chargeback_rpt.email_notifier as x_mail
from dateutil.relativedelta import relativedelta



def create_billing_report(df_all_costing,month_param,year_param):
    df_detail=df_all_costing['vm_detail']
    df_detail_storage=df_all_costing['storage_detail']
    df_nimble_detail=df_all_costing['nimble_detail']
    df_primera_detail=df_all_costing['primera_detail']



    currency_unit='(฿)'
    name_costType="Cost Type"
    sum_costType="Cost"+currency_unit
    total_costType="Total All Types"

    col_no_vm='VM Server(No.)'
    col_no_backup='Backup(No.)'
    col_size_storage=' Storage-UsedSize(GB)'

    onlyBackup_col='only_backup'
    cate_group=["cost_center", "system_name"]

    single_group=["system_name"]

    vmCostType_params=['vm_cost','backup_cost']
    strageCostType_params=['storage_cost']

    main_vm='vm'
    main_storage='size_used_gb'


    key_storage='storage'
    key_vm='vm'

    # nonlocal total_keyWords
    #total_keyWords=[total_costType,sub_total_2]


    # For show vm detail

    vm_show_cols=[main_vm,'system_name','cost_center','cpu','memory','capacity_gb','os','database']
    backup_show_cols=[main_vm,'system_name','cost_center','backup_size_gb']



    t_id=vm_util.creating_transaction(4,month_param,year_param)
    print(f"ETL Transaction ID: {t_id}")

    list_error=[]
    print(list_error)

    def check_error_point(tran_id):
      print(list_error)
      if True in list_error:


        vm_util.collect_error_to_sent_mail(tran_id)

        print("Create billing data occured error")

        raise Exception("Program is teminated and check error from email and log_error.txt")

      list_error.clear()
      print(list_error)

    print("copy vm dataframe to create detail later")
    df_master_detail=df_detail.copy()


    # This version for test only by getting from excel file
    # temp_path=os.path.abspath(r'D:\JupyterCode\Yit\ChargebackReport\report_data\Chargeback_Detail_12-2020.xlsx')
    # dfx_list=pd.read_excel(temp_path,sheet_name=['vm','storage'])
    def get_df_detail_from_file(dfx_dict,key_name,disp_cols,mappingDict_cols):
        dfx=dfx_dict[key_name]
        dfx=dfx[disp_cols]
        dfx=dfx.rename(columns=mappingDict_cols)
        return dfx

    # this verson for production by getting from method  chargeback_rpt.report_builder_detail as detail_rpt
    def get_df_detail(dfx,disp_cols,mappingDict_cols):
        dfx=dfx[disp_cols]
        dfx=dfx.rename(columns=mappingDict_cols)
        return dfx


    def internal_display_mapping(listInternalCols):
      #dictCols_DetailVM=dict(zip(listInternalCols_DetailVM,listDispCols_DetailVM))
     listDispCols=[]
     dictCols_mapping={}
     dictDisp_mapping={}

     for  x in  listInternalCols:
       sr_temp= vc.get_datafield_by_column_name(x)

       if sr_temp is not None:

        disp_col=sr_temp['column_display_name']
        col=sr_temp['column_table_name']
        listDispCols.append(disp_col)
        dictCols_mapping[disp_col]=col
        dictDisp_mapping[col]=disp_col

       else:
        # write error and add error to list
        list_error.append(True)
        error_message=f"column named { x} in report detail were not found in datafield_mapping table"
        print(error_message)
        vm_util.add_error_to_database(7,error_message,t_id)


     return  listDispCols,dictCols_mapping,dictDisp_mapping

    def listColumns_for_creatingReport(mainCols,cate_group,xCostTuye_params,otherCols=None):
     listInternalCols_Detail=mainCols+cate_group
     dispCols_DetailList,internnal_DetailDict,disp_DetailDict=internal_display_mapping(listInternalCols_Detail)

    #  print(dispCols_DetailList)
    #  print(internnal_DetailDict)
    #  print(disp_DetailDict)

     xCostType=vc.list_cost_type(xCostTuye_params)
     total_x=xCostType['report_name'].tolist()
     if otherCols is not None:
      x_cols=dispCols_DetailList+total_x+otherCols
     else:
      x_cols=dispCols_DetailList+total_x

    #  print(x_cols)
     return x_cols,internnal_DetailDict,disp_DetailDict

    def find_currency_unit(x,sub_x):

     if (x.find(sub_x) != -1):
        return x
     else:
        return f'{x}{sub_x}'

    try:
       print("Load Cost Type to bill report")

       vm_cost=vc.get_cost_type('vm_cost')['report_name']
       smr_vm_cost=vm_cost.replace(currency_unit,'').strip()

       backup_cost=vc.get_cost_type('backup_cost')['report_name']
       smr_backup_cost=backup_cost.replace(currency_unit,'').strip()



       print("Item for header")
       print(vm_cost)
       print(backup_cost)


       print("=========================================")
       print("Item for summary")
       print(smr_vm_cost)
       print(smr_backup_cost)


    except Exception as ex:
       list_error.append(True)
       error_message=f"not found either vm_cost, backup_cost or storage_cost in columne type_name in cost_type table"
       print(error_message)
       vm_util.add_error_to_database(19,error_message,t_id)


    #vm_backup_COST=f'Total VM + Backup(฿)'
    #print(vm_backup_COST)

    check_error_point(t_id)

    print("1.Generate Overview Report")

    print("VM Report")

    print("create columns for building vm report")
    vm_cols,internaVM_mapCols,dispVM_mapCols=listColumns_for_creatingReport([main_vm],cate_group,vmCostType_params,[onlyBackup_col])
    print(vm_cols)
    print(internaVM_mapCols)
    print(dispVM_mapCols)

    check_error_point(t_id)

    print("load data from detail report for building billing vm report")
    df_detail=get_df_detail(df_detail,vm_cols,internaVM_mapCols)

    df_detail_vm=df_detail[df_detail[onlyBackup_col]==False]
    df_detail_backup=df_detail[df_detail[backup_cost]>0]

    df_detail_vm=df_detail_vm.drop(columns=[onlyBackup_col])
    df_detail_backup=df_detail_backup.drop(columns=[onlyBackup_col])



    print("============================================================")
    print("vm info")
    print(df_detail_vm.head(10))
    print("backup info")
    print(df_detail_backup.head(10))

    def aggregate_vm_backup(df_detailX,df_detailY):
     print("aggregate for vm report")
     #as_index=False
     df_x=df_detailX.groupby(cate_group,as_index=False).agg({
      vm_cost:'sum',
     })

     df_y=df_detailY.groupby(cate_group,as_index=False).agg({
      backup_cost:'sum',
     })

    #  print("combine  vm and backup data")
    #  df_xy=pd.merge(df_x,df_y,left_on=cate_group,right_on=cate_group,how='outer').fillna(0)
    #  df_xy[vm_backup_COST]= df_xy[vm_cost]+ df_xy[backup_cost]
    #  print(df_xy)
    #  print("=================================================================")

     print(df_x.head(20))
     print("=================================================================")
     print(df_y.head(20))


    #  return df_x,df_y,df_xy
     return df_x,df_y


    df_vm,df_backup=aggregate_vm_backup(df_detail_vm,df_detail_backup)


    print("List Cost Center For VM and Backup")

    vm_cc_group=df_vm['cost_center'].unique()
    vm_cc_group

    backup_cc_group=df_backup['cost_center'].unique()
    backup_cc_group

    def build_storage_data_report(df_x_detail,x_model_name,x_storage_size,x_param,xCostType_params):

        print(f"1.load Cost Type to {x_model_name} report")

        x_cost=vc.get_cost_type(x_param)['report_name']
        smr_x_cost=x_cost.replace(currency_unit,'').strip()


        print(f"2.create columns for building {x_model_name } report ")
        x_cols,internal_mapCols,disp_mapCols=listColumns_for_creatingReport([x_storage_size],cate_group,xCostType_params)
        #print(x_cols)
        #print(internal_mapCols)
        #print(disp_mapCols)

        print(f"3.load data for building {x_model_name} report")
        df_x_detail=get_df_detail(df_x_detail,x_cols,internal_mapCols)
        #print(f"{x_model_name} info")
        #print(df_x_detail.head(10))

        print(f"4.aggregate for {x_model_name}  report")
        df_x=df_x_detail.groupby(cate_group,as_index=False).agg({
          x_cost:'sum',
        })
        #print(df_x)

        print(f"6.list Cost Center For {x_model_name} Storage")
        x_cc_group=df_x['cost_center'].unique()
        #print(x_cc_group)

        return df_x_detail,df_x,x_cc_group,x_cost,smr_x_cost,internal_mapCols,disp_mapCols


    print("NetApp")
    netapp_model_name='netapp'
    netapp_storage_size='size_used_gb'
    netapp_param='storage_cost'
    netappCostType_params=[netapp_param]

    if df_detail_storage is not None:
        print(df_detail_storage)
        df_detail_storage,df_storage,storage_cc_group,storage_cost,smr_storage_cost,internaSTR_mapCols,dispSTR_mapCols \
          =build_storage_data_report(df_detail_storage,netapp_model_name,netapp_storage_size,netapp_param,netappCostType_params)
        print("============================================================")
        print("NetApp data has been transformed")
        print(df_detail_storage)
        print(df_storage )
        print(storage_cost)
        print(smr_storage_cost)
        print(internaSTR_mapCols)
        print(dispSTR_mapCols)
        print(storage_cc_group)

    else:
        df_storage=None
        storage_cc_group=[]


    print("Nimble")
    nim_model_name='nimble'
    nim_storage_size='size_used_gb_nimble'

    nim_param='nimble_cost'
    nimCostType_params=[nim_param]

    if df_nimble_detail is not None:

        print(df_nimble_detail)


        df_nimble_detail,df_nim,nim_cc_group,nim_cost,smr_nim_cost,nim_internal_mapCols,nim_disp_mapCols \
        =build_storage_data_report(df_nimble_detail,nim_model_name,nim_storage_size,nim_param,nimCostType_params)


        print("============================================================")
        print("Nimble data has been transformed")
        print(df_nimble_detail)
        print( df_nim  )
        print(nim_cost)
        print(smr_nim_cost)
        print(nim_internal_mapCols)
        print(nim_disp_mapCols)
        print(nim_cc_group)

    else:
        df_nim=None
        nim_cc_group=[]



    print("Primera")
    prim_model_name='primera'
    prim_storage_size='size_used_gb_primera'

    prim_param='primera_cost'
    primCostType_params=[prim_param]

    if  df_primera_detail is not None:

        print(df_primera_detail)
        print("============================================================")
        print("Primera data has been transformed")
        df_primera_detail,df_prim,prim_cc_group,prim_cost,smr_prim_cost,prim_internal_mapCols,prim_disp_mapCols= \
        build_storage_data_report(df_primera_detail,prim_model_name,prim_storage_size,prim_param,primCostType_params)


        print(df_primera_detail)
        print(df_prim )
        print(prim_cost)
        print(smr_prim_cost)
        print(prim_internal_mapCols)
        print(prim_disp_mapCols)
        print(prim_cc_group)

    else:
        print("No data")
        df_prim=None
        prim_cc_group=[]


    print("Create Expenditure Summary By Cost Type")

    cost_summary_data=[]
    cost_summary_index=[]

    if df_vm is not None:
      cost_summary_data.append(df_vm[vm_cost].sum())
      cost_summary_index.append(smr_vm_cost)

    if  df_backup is not None:
      cost_summary_data.append(df_backup[backup_cost].sum())
      cost_summary_index.append(smr_backup_cost)

    if  df_storage is not None:
      cost_summary_data.append(df_storage[storage_cost].sum())
      cost_summary_index.append(smr_storage_cost)

    if  df_nim is not None:
      cost_summary_data.append(df_nim[nim_cost].sum())
      cost_summary_index.append(smr_nim_cost)

    if  df_prim is not None:
      cost_summary_data.append(df_prim[prim_cost].sum())
      cost_summary_index.append(smr_prim_cost)



    df_costTypeSummary= (pd.Series(cost_summary_data, index =cost_summary_index)).to_frame(name=sum_costType)
    sum_allCostType=df_costTypeSummary[sum_costType].sum()

    df_costTypeSummary.index.name=name_costType
    df_costTypeSummary.reset_index(inplace=True)


    print(df_costTypeSummary)

    print("Total Cost of BDMS")
    print(sum_allCostType)

    print("Set Overview Report Data to Dict")
    try:
     print("Get BDMS Contact Info Overview")


     overview_site=vm_util.get_config_value("main_cost_center",t_id)
     cc_info_sr=vm_util.get_cost_center(overview_site,t_id)

     print(cc_info_sr)

      # dict to render data in html file
     overview_dict={

                         "CostCenterInfo":cc_info_sr,
                         "Total_AllCost" :str(sum_allCostType) ,
                         "Total_ByCateCost": df_costTypeSummary,
                         "VM_CC": vm_cc_group,
                         "VM_Cost": df_vm ,
                         "Backup_CC":backup_cc_group,
                         "Backup_Cost": df_backup,
                         "Storage_CC":storage_cc_group,
                         "Storage_Cost":df_storage ,
                         "Nimble_CC":nim_cc_group,
                         "Nimble_Cost":df_nim ,
                         "Primera_CC":prim_cc_group,
                         "Primera_Cost":df_prim ,
     }
    except Exception as ex:
     list_error.append(True)




    check_error_point(t_id)

    print("======================Completed to create overview report=============================")

    print("2.Generate Report For Each Cost Center")
    CC_Dict_Report={}

    print("list columns to show in VM Summary")
    CC_VM_SumList=[col_no_vm,vm_cost]
    print(CC_VM_SumList)

    print("list columns to vm in detail")
    vm_show_DispCols,to_internale_map,to_disp_map= internal_display_mapping(vm_show_cols)

    vm_show_DispCols=vm_show_DispCols+[vm_cost]

    print("VM Show_DispCols")
    print(vm_show_DispCols)
    print(to_internale_map)
    print(to_disp_map)

    print("list columns to show in Backup Summary")
    CC_Backup_SumList=[col_no_backup,backup_cost]
    print(CC_Backup_SumList)


    print("list columns to backup in detail")
    backup_show_DispCols, backup_to_internale_map, backup_to_disp_map= internal_display_mapping(backup_show_cols)

    backup_show_DispCols=backup_show_DispCols+[backup_cost]

    print("BACKUP Show_DispCols")
    print(backup_show_DispCols)
    print(backup_to_internale_map)
    print(backup_to_disp_map)


    print("Cost ceter by product")
    print("VM Group : ", vm_cc_group)
    print("Backup Group ", backup_cc_group)
    print("NetApp Group : ", storage_cc_group)
    print("Nimble Group : ",nim_cc_group)
    print("Premira Group : ", prim_cc_group)


    cc_List= list(set().union(vm_cc_group, backup_cc_group,storage_cc_group,nim_cc_group,prim_cc_group))
    print(f"============================All-{len(cc_List)}===================================================")
    print(cc_List)

    # print("For test")
    # cc_List=['BHQ','GLS7180','GLS1000','GLS7444','unknown']
    # print(cc_List)

    def aggregate_vm_backup_EachCostCenter(df_detailX,df_detailY):

     #as_index=False
     # df_x for vm
     df_x=df_detailX.groupby(single_group,as_index=False).agg({
      main_vm:'count' ,
      vm_cost:'sum'

     })
     df_x=df_x.rename(columns={main_vm:col_no_vm})

     # df_y for backup
     df_y=df_detailY.groupby(single_group,as_index=False).agg({
      main_vm:'count' ,
      backup_cost:'sum'

     })
     df_y=df_y.rename(columns={main_vm:col_no_backup})


     return df_x,df_y


    def aggregate_storage_EachCostCenter(df_detail_x,cc_param,size_x,cost_x,model_name):
     print(f"Aggregate {model_name} for {cc_param}")
     df_detail_by_cc= df_detail_x.query('cost_center==@cc_param').copy()
     print(f"1.Filter {model_name.title()} Storage data by {cc_param}")
     print(df_detail_by_cc.tail(5))

     print(f"2.Group {model_name.title()} by {size_x} and {cost_x}")
     dfCC_x=df_detail_by_cc.groupby(single_group,as_index=False).agg({
      size_x: 'sum' ,
      cost_x: 'sum'
     })
     dfCC_x=dfCC_x.rename(columns={size_x:col_size_storage})

     return  dfCC_x



    def get_detail(cost_center_name,df_show_detail,x_to_disp_map,x_show_DispCols,x_cost):


     cc_col=x_to_disp_map['cost_center']
     # fitter cost !=0
     df_show_detail=df_show_detail[ (df_show_detail[cc_col]==cost_center_name) & (df_show_detail[x_cost]>0)   ]


     df_show_detail=df_show_detail.loc[:,x_show_DispCols]

     df_show_detail = df_show_detail.fillna("-")
     df_show_detail.set_index(x_to_disp_map[main_vm],inplace=True)

     df_show_detail=df_show_detail.drop(columns=[cc_col])
     df_show_detail.reset_index(inplace=True)

     #print(df_show_detail.info())
     #print(df_show_detail)

     return df_show_detail



    cc_dictList=[]
    for cc_param in cc_List:
     try:

      # summarise each one of of cost type
      ecah_cc_info_sr=vm_util.get_cost_center(cc_param,t_id)
      #print(ecah_cc_info_sr)
      print(f"####################Start Creating Report For {cc_param}######################################")
      print('Cost Ceneter: ',cc_param)

      ccSummary_type=[]
      ccSummary_cost=[]

      print(f"=================================Filter VM and Backup by {cc_param}=============================")
      print(f"Filter VM data by {cc_param}")
      df_cc_vm= df_detail_vm.query('cost_center==@cc_param').copy()
      print(df_cc_vm.tail(10))
      print(f"Filter Backup data by {cc_param}")
      df_cc_backup= df_detail_backup.query('cost_center==@cc_param').copy()
      print(df_cc_backup.tail(10))

      print(f"Aggregate VM and Backup for {cc_param}")
      dfCC_vm,dfCC_backup=aggregate_vm_backup_EachCostCenter(df_cc_vm,df_cc_backup)

      print(dfCC_vm)
      if  not dfCC_vm.empty:
         print("VM Summary")
         ccSummary_type.append(smr_vm_cost)
         ccSummary_cost.append(dfCC_vm[vm_cost].sum())
      else:
         print("No VM Summary")


      print(dfCC_backup)
      if  not dfCC_backup.empty:
         print("Backup Summary")
         ccSummary_type.append(smr_backup_cost)
         ccSummary_cost.append(dfCC_backup[backup_cost].sum())
      else:
        print("No Backup Summary")

      #=================================Storage=============================


      if df_detail_storage is not None:

          df_cc_storage= aggregate_storage_EachCostCenter(df_detail_storage,cc_param,main_storage,storage_cost,netapp_model_name)
          print(df_cc_storage)
          if  not df_cc_storage.empty:
             ccSummary_type.append(smr_storage_cost)
             ccSummary_cost.append(df_cc_storage[storage_cost].sum())

      else:
          print("No NetApp Summary")
          df_cc_storage=pd.DataFrame()

      if df_nimble_detail is not None:
          print("Nimble Summary")
          df_cc_nimble= aggregate_storage_EachCostCenter(df_nimble_detail,cc_param,nim_storage_size,nim_cost,nim_model_name)
          print(df_cc_nimble)
          if not df_cc_nimble.empty:
             ccSummary_type.append(smr_nim_cost)
             ccSummary_cost.append(df_cc_nimble[nim_cost].sum())
      else:
          print("No Nimble Summary")
          df_cc_nimble=pd.DataFrame()

      if    df_primera_detail is not None:
          print("Primera Summary")
          df_cc_primera= aggregate_storage_EachCostCenter(df_primera_detail,cc_param,prim_storage_size,prim_cost,prim_model_name)
          print(df_cc_primera)
          if not df_cc_primera.empty:
             ccSummary_type.append(smr_prim_cost)
             ccSummary_cost.append(df_cc_primera[prim_cost].sum())
      else:
         print("No Primera Summary")
         df_cc_primera=pd.DataFrame()

      print("************************************************")
      print(f"Summarize Cost By Type For Cost Center {cc_param}")
    #   ccSummary_type=[smr_vm_cost,smr_backup_cost,smr_storage_cost]
    #   ccSummary_cost=[dfCC_vm[vm_cost].sum(),dfCC_backup[backup_cost].sum(),df_cc_storage[storage_cost].sum()]
      dfCC_costTypeSummary=pd.DataFrame(data={name_costType:ccSummary_type,sum_costType:ccSummary_cost})
      print(dfCC_costTypeSummary)

      sumCC_allCostType=dfCC_costTypeSummary[sum_costType].sum()
      print("Total Cost of ",cc_param,' :',sumCC_allCostType )



      #if dfCC_costTypeSummary.iloc[0,1] >0 :
      print(f"List Detail VM  in {cc_param}")
      dfVM_Detail=get_detail(cc_param,df_master_detail,to_disp_map,vm_show_DispCols,vm_cost)
      print(dfVM_Detail.head())


      #if dfCC_costTypeSummary.iloc[1,1] >0 :
      print(f"List Detail Backup in {cc_param}")
      dfBackup_Detail=  get_detail(cc_param,df_master_detail,backup_to_disp_map,backup_show_DispCols,backup_cost)
      print(dfBackup_Detail.head())


      # dict to render data in html file
      print(f"Collection data as Dictionary for {cc_param}")
      x_dict= {

                     "CostCenterInfo":ecah_cc_info_sr,

                     "Total_AllCost" :str(sumCC_allCostType) ,
                     "Total_ByCateCost":dfCC_costTypeSummary ,

                     "VM_Cost": dfCC_vm,
                     "Backup_Cost":dfCC_backup,
                     "Storage_Cost":df_cc_storage ,
                     "Nimble_Cost":df_cc_nimble,
                     "Primera_Cost":df_cc_primera,

                     "VM_Detail":dfVM_Detail,
                     "Backup_Detail":dfBackup_Detail

                    }

      cc_dictList.append({cc_param:x_dict})



      print(f"####################Finished Creating Report For {cc_param}############################")

     except Exception as ex:
      list_error.append(True)
      error_message=f"Found someting wrong while looping cost center report as detail belows for  {cc_param}"
      print(f'{error_message} :{str(ex)}')
      vm_util.add_error_to_database(20,error_message,t_id)


    check_error_point(t_id)
    print("======================Completed to create each cost center report=============================")

    try:

        updated_rows=vm_util.created_transaction(t_id)
        print("completed building billing data")

    except Exception as ex:
        list_error.append(True)
        print(ex)

    check_error_point(t_id)

    return overview_dict,cc_dictList


