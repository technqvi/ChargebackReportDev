import os
import zipfile 
import datetime
import shutil 

def make_zip(report_diectory,list_fileType,zip_file):
 try:   
  x_dir_back='.'
  compression = zipfile.ZIP_DEFLATED
  with zipfile.ZipFile(zip_file,'w',zipfile.ZIP_DEFLATED) as zip: 
   for root, dirs, files in os.walk(report_diectory):
    for file in files:
      name, extension = os.path.splitext(file)  
      if extension in list_fileType:
        #print(file)
        zip.write(os.path.join(root, file), os.path.relpath(os.path.join(root, file), os.path.join(report_diectory, x_dir_back)),compress_type=compression)
   
   return True
 except Exception as ex:
    raise ex
        
 


def delele_file(file_path):
 try:   
    os.remove(file_path)
    return True
 except Exception as ex:
    raise ex
    

def delete_entire_directory(directory_path):   
 try:
     shutil.rmtree(directory_path)
     return True
 except OSError as ex:
    raise ex

    

def create_directory(directory_path):

 try:  
    if not os.path.exists(directory_path):
     os.mkdir(directory_path)  
     return True
    else :
     return False   
 except OSError as error:  
    raise error

def move_file(source_file,target_directory):
 try:
   if os.path.exists(source_file) and  os.path.exists(target_directory):
    shutil.move(source_file,target_directory) 
   else:
    raise Exception(f"not found some paths either {source_file} or {target_directory}")  
 except Exception as error:  
   raise error  
    


def rename_file(original_file,new_file):
 try:
    if os.path.exists(original_file) : 
     os.rename(original_file, new_file)
    else:
     raise Exception(f"not found {original_file}") 
 except Exception as error:  
   raise error


def is_notEmpty_folder(path):
    x = False
    try:

        if os.path.exists(path) and not os.path.isfile(path):
            x = x + True
            if os.listdir(path):
                x = x + True

    except Exception as error:
        raise error

    return int(x)