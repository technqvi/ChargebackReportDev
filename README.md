# About
* This repository is development phase of [ChargeBackApp](https://github.com/technqvi/ChargeBackApp)
* The Chargeback System is able to retrieve the following private cloud 's resources usage  to calculate monthly expense. 
  - VM Usage(CPU,Memory,Disk Capacity, OS and Database)
  - Storage(Volume Usage Size) such as NetApp,HPE Storage(Nimble and Primera  and StoreOnce)
* It will charge each CostCenter(1 CostCenter is representative of  Business Unit) for IT-Infrastructure resources usage.
* System will generate monthly billing report as pdf file and send it to each the cost center via email.


## VM-ChargeBack Process Flow
![chargeback_overview](https://github.com/technqvi/ChargebackReportDev/assets/38780060/1757c3ef-2039-494c-9f5d-e7c8ccd51d99)
#### 1.Import data
The system will get Cloud-Infrastructure resources usage such as VM-Instances,NetApp-Storage,HPE-Storage and StoreOnce  and import usage data to the specific local path as csv file / excel file.
#### 2.ETL Data 
Extract, transform and load(ETL) data from csv or excel files to postgresql database. the follwing steps are some of the ETL processing
- removing duplicate row and null values.
- filtering by specific connction.
- adding new columns.

#### 3.Calculate Monthly Cost
There are 3 cost calculation categories as below.
1. Master Cost: 
   - VM Cost:  CPUs,Memory,Disk Size
   - Storage Cost(NetApp,HP Storage): Storage Size
2. Database & OS Cost: Instances/Server Base , No.License Base and Core-CPU to Instance Base.
3. Operation & Maintenance Cost: Antivirus Software,IT-Operation Service,Performance Monitoring Alert,Backup Software(Depending on Disk Backup Size) and System Monitoring Service 

#### 4.Build Report 
- Use cost calculation from the previous step to summarize expenses by cost center.
- Generate Microsoft Excel and pdf reports and collect these files as a zip file and attach a ZIP File to an Email and send it out. 

#### 5.Check valid data (Addtional Module)
Check  data from VM,Storage Server and check data is ready and valid to save into database. if any error, the system will email to admin. This step is used for getting data ready to perform ETL data step.

## StoreOnce ChargeBack Process 
Mainly , program is similar to VM-ChargeBack System except ETL step that it can pull data data from StoreOnce server to database through StoreOnce-API directly without file like CSV/Excel .  
![storeonce_overview](https://github.com/technqvi/ChargebackReportDev/assets/38780060/d2f7e0fa-3512-4d5b-8cd1-62e8449cf8b7)
## Web Administrator
<img width="945" alt="Chargeback-App-Web-Admin" src="https://github.com/technqvi/ChargebackReportDev/assets/38780060/ddd37cb9-dce4-4d16-b94d-662bcca25138">

### PDF-Report as output
![258594409-ecccd741-6ac8-41b4-9b8a-fdac6582879c](https://github.com/technqvi/ChargebackReportDev/assets/38780060/3570c753-0c74-4c04-aa86-a551152a2ac6)

#  SourceCode
## ETL Data To Database

### etl_vm_data.ipynb
* Load VM-Instances usage data from csv file in order to perform data cleansing and transformation and  save data into database.
* Table Data Schema : cpu,memorycapacity_gb,os,database,month,year,costcenter,system name, created_date

### etl_storage_data_netapp.ipynb
* Load NetApp usage data from excel file in order to perform data cleansing and transformation and  save data into database.
* Table Data Schema : volume_name,size_used_gb,month,year,costcenter,system name, created_date.

### etl_storage_data_hpe.ipynb
* Load Primera-Storage or Nimble-Storage usage data from csv file in order to perform data cleansing and transformation and  save data into database.
* Table Data Schema : volume_name,size_used_gb,month,year,costcenter,system name, created_date.

### etl_storage_data_storeonce.ipynb
* Load StoreOnce Storage usage data from StoreOnce directly in order to perform data cleansing and transformation and  save data into database.
* Table Data Schema : user_size,disk_size,month,year,catalyst_name, created_date

## Calculate Usage Cost and Build Report
### dev_report_builder_costing.ipynb
* Load Usage data such as VMs,NetApp-Storage,HPE-Storage  from database.
* Load Unit-Price of each usage cost type  such as Memory,CPUs,VMs Disk Size  and Storage Disk Size , OS/Dabase licensing Price, Infrastructure Support/Maintainance (Antivirus,Performance Monitoring,Backup).
* Calculate Cost based on service type such as VMs,NetApp-Storage,HPE-Storage  as Dataframe  to aggregate the cost  summary of report furthter in the another process.

### dev_report_builder_billing.ipynb
* Take dataframe from dev_report_builder_costing.ipynb to aggregate the cost grouped by cost center and system name.
*  There are 2 kind of output dataframe  : 1.Total Cost Report 2.CostCenter 's Cost Report

### dev_monthly_report_builder.ipynb | run_report.ipynb
* Generate Excel file  from dataframe return in dev_report_builder_costing.ipynb
* Generate Pdf file  from datafram return in  dev_report_builder_billing.ipynb
* Compress all files from ealier step  into Zip.
* Attach zip file and send mail.

### primera_export_yit_py.ipynb
* pull Premera usage data from HPE Storage Server through Python Client API.

### report_builder_item_costing.ipynb
* Load Usage StoreOnce data  and Unit-Price of storeonce usage from database.
* Calcualte Cost base on usage size and generate PDF file.
* Compress all files from into Zip file and attach zip file and send mail.



##  Library/Module and Miscellaneous Utility 
#### chargeback_rpt
* report_builder_billing.py | report_builder_costing.py | monthly_report_builder.py :
These files are the same as  ipynb file but they are converted to py files to be executable on python environment.

*  vm_data_utility.py | vm_data_validator.py | vm_data_charger.py :
These files are used to process logic calculation , data validation , database manaegment to build chagedback report.

* db_postgres_command.py| email_notifier.py | dev_email_notifier.ipynb | file_directory_manager.py :
They are about CRUD postgresql database , email, and file system.

