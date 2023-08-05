# About
* The Chargeback System is able to retrieve the following private cloud 's resources usage  to calculate monthly expense. 
  - VM Usage(CPU,Memory,Disk Capacity, OS and Database)
  - Storage(Volume Usage Size) such as NetApp,HPE Storage(Nimble and Primera  and StoreOnce)
* It will charge each CostCenter(1 CostCenter is representative of  Business Unit) for IT-Infrastructure resources usage.
* System will generate monthly billing report as pdf file and send it to each the cost center via email.

This repository is development phase of [ChargeBackApp](https://github.com/technqvi/ChargeBackApp)

## ETL Data 
We schedule job to run script in order to load data from VMWare(RVTool),NetApp(NetAppDoc3.6),HPE-Storeage(HPETookkit),StoreOnce(StoreOnce-API) and save them as csv/excel file to get data ready for ETL process.



### [etl_vm_data.ipynb](https://github.com/technqvi/ChargebackReport/blob/master/etl_vm_data.ipynb)
* Load VM-Instances usage data from csv file in order to perform data cleansing and transformation and  save data into database.
* Table Data Schema : cpu,memorycapacity_gb,os,database,month,year,costcenter,system name, created_date

### [etl_storage_data_netapp.ipynb](https://github.com/technqvi/ChargebackReport/blob/master/etl_storage_data_netapp.ipynb)
* Load NetApp usage data from excel file in order to perform data cleansing and transformation and  save data into database.
* Table Data Schema : volume_name,size_used_gb,month,year,costcenter,system name, created_date.

### [etl_storage_data_hpe.ipynb](https://github.com/technqvi/ChargebackReport/blob/master/etl_storage_data_hpe.ipynb)
* Load Primera-Storage or Nimble-Storage usage data from csv file in order to perform data cleansing and transformation and  save data into database.
* Table Data Schema : volume_name,size_used_gb,month,year,costcenter,system name, created_date.

### [etl_storage_data_storeonce.ipynb](https://github.com/technqvi/ChargebackReport/blob/master/etl_storage_data_storeonce.ipynb)
* Load StoreOnce Storage usage data from StoreOnce directly in order to perform data cleansing and transformation and  save data into database.
* Table Data Schema : user_size,disk_size,month,year,catalyst_name, created_date

## Calculate Usage Cost and Build Report
### [dev_report_builder_costing.ipynb](https://github.com/technqvi/ChargebackReport/blob/master/dev_report_builder_costing.ipynb)
* Load Usage data such as VMs,NetApp-Storage,HPE-Storage  from database.
* Load Unit-Price of each usage cost type  such as Memory,CPUs,VMs Disk Size  and Storage Disk Size , OS/Dabase licensing Price, Infrastructure Support/Maintainance (Antivirus,Performance Monitoring,Backup).
* Calculate Cost based on service type such as VMs,NetApp-Storage,HPE-Storage  as Dataframe  to aggregate the cost  summary of report furthter in the another process.

### [dev_report_builder_billing.ipynb](https://github.com/technqvi/ChargebackReport/blob/master/dev_report_builder_billing.ipynb)
* Take dataframe from [dev_report_builder_costing.ipynb](https://github.com/technqvi/ChargebackReport/blob/master/dev_report_builder_costing.ipynb) to aggregate the cost grouped by cost center and system name.
*  There are 2 kind of output dataframe  : 1.Total Cost Report 2.CostCenter 's Cost Report

### [dev_monthly_report_builder.ipynb](https://github.com/technqvi/ChargebackReport/blob/master/dev_monthly_report_builder.ipynb) | [run_report.ipynb](https://github.com/technqvi/ChargebackReport/blob/master/run_report.ipynb)
* Generate Excel file  from dataframe return in [dev_report_builder_costing.ipynb](https://github.com/technqvi/ChargebackReport/blob/master/dev_report_builder_costing.ipynb)
* Generate Pdf file  from datafram return in  [dev_report_builder_billing.ipynb](https://github.com/technqvi/ChargebackReport/blob/master/dev_report_builder_billing.ipynb)
* Compress all files from ealier step  into Zip.
* Attach zip file and send mail.

### [primera_export_yit_py.ipynb](https://github.com/technqvi/ChargebackReport/blob/master/primera_export_yit_py.ipynb)
* pull Premera usage data from HPE Storage Server through Python Client API.

### [report_builder_item_costing.ipynb](https://github.com/technqvi/ChargebackReport/blob/master/report_builder_item_costing.ipynb)
* Load Usage StoreOnce data  and Unit-Price of storeonce usage from database.
* Calcualte Cost base on usage size and generate PDF file.
* Compress all files from into Zip file and attach zip file and send mail.



##  Library/Module and Miscellaneous Utility 
#### [chargeback_rpt](https://github.com/technqvi/ChargebackReport/tree/master/chargeback_rpt)

* [report_builder_billing.py](https://github.com/technqvi/ChargebackReport/blob/master/chargeback_rpt/report_builder_billing.py) | [report_builder_costing.py](https://github.com/technqvi/ChargebackReport/blob/master/chargeback_rpt/report_builder_costing.py) | [monthly_report_builder.py](https://github.com/technqvi/ChargebackReport/blob/master/chargeback_rpt/monthly_report_builder.py) :
These files are the same as  ipynb file but they are converted to py files to be executable on python environment.

*  [vm_data_utility.py](https://github.com/technqvi/ChargebackReport/blob/master/chargeback_rpt/vm_data_utility.py) | [vm_data_validator.py](https://github.com/technqvi/ChargebackReport/blob/master/chargeback_rpt/vm_data_validator.py) | [vm_data_charger.py](https://github.com/technqvi/ChargebackReport/blob/master/chargeback_rpt/vm_data_charger.py) :
These files are used to process logic calculation , data validation , database manaegment to build chagedback report.

* [db_postgres_command.py](https://github.com/technqvi/ChargebackReport/blob/master/chargeback_rpt/db_postgres_command.py) | [email_notifier.py](https://github.com/technqvi/ChargebackReport/blob/master/chargeback_rpt/email_notifier.py) | [dev_email_notifier.ipynb](https://github.com/technqvi/ChargebackReport/blob/master/dev_email_notifier.ipynb) | [file_directory_manager.py](https://github.com/technqvi/ChargebackReport/blob/master/chargeback_rpt/file_directory_manager.py) :
They are about CRUD postgresql database , email, and file system.

#### [test_code](https://github.com/technqvi/ChargebackReport/tree/master/test_code) | [test_etl_data.py](https://github.com/technqvi/ChargebackReport/blob/master/test_etl_data.py)  : these files are test scripts.