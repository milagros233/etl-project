import glob 
import pandas as pd 
import xml.etree.ElementTree as ET 
from datetime import datetime 


log_file = "log_file.txt" 
target_file = "transformed_data.csv" 

#extract
def extract_from_csv(file_to_process):
    dataframe= pd.read_csv(file_to_process)
    return dataframe

def extract_from_json(file_to_process):
    dataframe= pd.read_json(file_to_process,lines=True)
    return dataframe

def extract_from_xml(file_to_process):
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    data = []

    for person in root:
        name = person.find("name").text
        height = person.find("height").text
        weight = person.find("weight").text
        data.append({'name': name, 'height': height, 'weight': weight})

    dataframe = pd.DataFrame(data)  # Cambié 'Dataframe' a 'DataFrame' (mayúscula)
    return dataframe

def extract():
    extract_data= pd.DataFrame(columns=['name','height','weight'])

    for csvfile in glob.glob("*.csv"):
        extract_data= pd.concat([extract_data,pd.DataFrame(extract_from_csv(csvfile))], ignore_index=True)

    for jsonfile in glob.glob("*.json"):
        extract_data=pd.concat([extract_data,pd.DataFrame(extract_from_json(jsonfile))], ignore_index=True)

    for xmlfile in glob.glob("*.xml"):
        extract_data=pd.concat([extract_data,pd.DataFrame(extract_from_xml(xmlfile))], ignore_index=True)

    return extract_data

#transform
def transform(data):
    #pulgadas a metros
    data['height']= pd.to_numeric(data['height'], errors='coerce')
    data['height']= round((data['height'])*0.0254,2)

    #libra a kg
    data['weight']=pd.to_numeric(data['weight'], errors='coerce')
    data['weight']=round((data['weight'])*0.453592,2)

    return data

#load
def load_data(target_file,transformed_data):
    transformed_data.to_csv(target_file)

def log_progress(message):
    timestamp_format= '%Y-%h-%d-%H:%M:%S'
    now= datetime.now()
    timestamp= now.strftime(timestamp_format)

    with open(log_file,'a') as f:
        f.write(timestamp +' , '+ message + '\n')


#testing
log_progress("ETL Job Started")
log_progress("extraction")
df_extract= extract()
log_progress("transformation")
df_transform= transform(df_extract)
log_progress("load")
load_data(target_file,df_transform)
log_progress("ETL Job End")
