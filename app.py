import pandas as pd
from simple_salesforce import Salesforce, SFType
from connect import connect_to_salesforce
import random
import os
from salesforce_bulk import CsvDictsAdapter
from salesforce_bulk import SalesforceBulk
from simple_salesforce.exceptions import SalesforceMalformedRequest
from dotenv import load_dotenv


def read_project_template_csv():
    project_template_file = pd.read_csv("assets/" + load_dotenv("PROJECT_TEMPLATE_FILENAME"))
    return project_template_file

def get_project_template_csv_name():
    file_name = os.path.basename("assets/" + load_dotenv("PROJECT_TEMPLATE_DATA_FILENAME"))
    return os.path.splitext(file_name)[0]

def read_project_template_name_csv():
    project_template_file = pd.read_csv("assets/" + load_dotenv("PROJECT_TEMPLATE_DATA_FILENAME"))
    return project_template_file

def get_fields_to_create_for_project_object(project_template_file, sf):
    mdapi = sf.mdapi
    fields = []
    records = []
    picklist_values = []
    for index, row in project_template_file.iterrows():
        if row["Type"] == "Milestone" :
            custom_fields = {
                f"label_field_1": str(row['Activity Name']).replace(" ", "_") + "_Milestone_A",
                f"type_field_1": mdapi.FieldType("Date"),
                f"label_field_2": str(row['Activity Name']).replace(" ", "_") + "_Milestone_F",
                f"type_field_2": mdapi.FieldType("Date")
            }
            fields.append(custom_fields)

            record = {
                "OwnerId": "005C7000000IXpvIAG",
                "Name": str(row['Activity Name']),
                "sitetracker__Actual_Field_API_Name__c": str(row['Activity Name']).replace(" ", "_") + "_Milestone_A__c",
                "sitetracker__Critical_Milestone__c": False,
                "sitetracker__Forecast_Field_API_Name__c": str(row['Activity Name']).replace(" ", "_") + "_Milestone_F__c",
                "sitetracker__Active__c": False,
                "sitetracker__Activity_Type__c": "Milestone",
                "sitetracker__Approval_Status__c": "Approved",
                "sitetracker__Description__c": "Imported",
                "sitetracker__Email_on_Activity_Start__c": False,
                "sitetracker__Enable_Promise_Date__c": False,
                "sitetracker__Financial__c": False,
                "sitetracker__Optional__c": False,
                "sitetracker__Propagate_Milestone_Name__c": False,
                "sitetracker__Variant__c": False,
            }

            records.append(record)

            print(fields)
        
        if row["Type"] == "Picklist":

            picklist_values_raw = str(row["Picklist_Value"]).split("#")

            is_default = True

            for value in picklist_values_raw:
                picklist_values.append({
                                'fullName': str(value),
                                'default': is_default ,
                                'label': str(value)
                            })
                
                is_default = False

            object_api_name = 'sitetracker__Project__c'
            field_api_name = str(row['Activity Name']).replace(" ", "_") + "_Picklist"

            url = 'tooling/sobjects/CustomField/'

            payload = {
                "Metadata":
                    {
                        "type": mdapi.FieldType("Picklist"), 
                        "inlineHelpText": "", 
                        "label": f"{field_api_name}", 
                        "required": False, 
                        'valueSet': {
                            'restricted': True,
                            'valueSetDefinition': {
                                    'sorted': False,
                                    # picklist entries
                                    'value': picklist_values
                                }
                        }
                    },
                "FullName": f"{object_api_name}.{field_api_name}__c"
            }



            try:
                result = sf.restful(url, method='POST', json=payload)

                print(result)

                activity_name = row['Activity Name']
                activity_field_api_name = field_api_name + "__c"

                data = [{activity_name, activity_field_api_name}]

                print("\nPicklist Activity Name, API Name:")
                print(data)

            except SalesforceMalformedRequest as e:
                print(type(e.content))
                print(e.content)
                if str(e.content).find("There is already a field named") != -1:
                    pass
                else:
                    print("Something went wrong!")
                    exit()



    return fields, records

def insert_records_into_project_milestone_object(records, bulk):
    job = bulk.create_insert_job("sitetracker__Project_Milestone__c", contentType='CSV')
    csv_iter = CsvDictsAdapter(iter(records))
    batch = bulk.post_batch(job, csv_iter)
    bulk.wait_for_batch(job, batch)
    bulk.close_job(job)
    result = bulk.get_batch_results(batch)
    result_str = str(result)
    startIndex = result_str.find("id='") + 4
    ids = []
    for i in range(len(records)):
        ids.append(result_str[startIndex: startIndex+18])
        startIndex += 81

    # print(batch)
    # print(ids)

    data = []

    for id, record in zip(ids, records):
        data.append({"Id": id, "Name": record["Name"]})


    print("\nProject Milestone records(Record Id, Activity Name): ")
    print(data)

    # Query records based on Id to collect the 

    # ids_str = str(ids)
    # ids_str_query = ids_str[1:len(ids_str)-1]
    # query = f"SELECT Id, Name FROM sitetracker__Project_Milestone__c WHERE Id IN ({ids_str_query})"
    # print(query)
    # results = sf.query_all_iter(query)
    # for row in results:
    #     print(row)
   
    print("Done. Records uploaded.")

    return data

def create_custom_fields_for_project_object(fields, sf):

    mdapi = sf.mdapi

    for field in fields:
    
        object_api_name = 'sitetracker__Project__c' 

        field_api_name_1 = field["label_field_1"] 
        field_label_1 = field["label_field_1"] 
        
        field_api_name_2 = field["label_field_2"] 
        field_label_2 = field["label_field_2"] 
        
        url = 'tooling/sobjects/CustomField/'

        payload_1 = {
            "Metadata":
                {"type": mdapi.FieldType("Date"), "inlineHelpText": "", "label": f"{field_label_1}", "required": False},
            "FullName": f"{object_api_name}.{field_api_name_1}__c"
        }

        payload_2 = {
            "Metadata":
                {"type": mdapi.FieldType("Date"), "inlineHelpText": "", "label": f"{field_label_2}", "required": False},
            "FullName": f"{object_api_name}.{field_api_name_2}__c"
        }

        try:
            result_1 = sf.restful(url, method='POST', json=payload_1)
            print('result 1:', result_1)

            result_2 = sf.restful(url, method='POST', json=payload_2)
            print('result 2:', result_2)
        except SalesforceMalformedRequest as e:
            print(type(e.content))
            print(e.content)
            if str(e.content).find("There is already a field named") != -1 :
               pass
            else:
                print("Something went wrong!")
                exit()


def insert_project_template_record(session_id, instance, sf):

    project_template_df = read_project_template_csv()
    name = get_project_template_csv_name()

    # Define the object and record data
    project_template = 'sitetracker__Project_Template__c'
    record_data = {
        'OwnerId': project_template_df.OwnerId.values.any(),
        'Name':  "Temp_" + str(name)  + "_" + str(random.randint(0, 1000)),
        'sitetracker__Active__c': bool(project_template_df.sitetracker__Active__c.values.any()),
        'sitetracker__Allow_Updates_to_Rendered_Items_Only__c': bool(project_template_df.sitetracker__Allow_Updates_to_Rendered_Items_Only__c.values.any()),
        'sitetracker__Schedule_Show_Duration__c': bool(project_template_df.sitetracker__Schedule_Show_Duration__c.values.any()),
        'sitetracker__Schedule_Show_WBS__c': bool(project_template_df.sitetracker__Schedule_Show_WBS__c.values.any()),
        'sitetracker__Schedule_Use_Layouts__c': bool(project_template_df.sitetracker__Schedule_Use_Layouts__c.values.any()),
        'sitetracker__SiteTraker_Object__c': project_template_df.sitetracker__SiteTraker_Object__c.values.any()
    }

    print(record_data)

    try:

        # Insert the record

        sf_project_template_object = SFType('sitetracker__Project_Template__c', session_id, instance)
        sf_project_template_object.create(record_data)

        query = f"SELECT Id FROM {project_template} WHERE Name = '{record_data.get('Name')}'"

        data = sf.query(query)

        print(data)

        # print(str(list(data.items())[2]))
        startIndex = str(list(data.items())[2]).find("Id")+6
        endIndex = startIndex+18
        Id = str(list(data.items())[2])[ startIndex:endIndex] 

        print("Project_Template__c record ID: ")
        print(Id)

        return Id

    except SalesforceMalformedRequest as e:
            print(type(e.content))
            print(e.content)
            if str(e.content).find("There is already a field named") != -1:
               pass
            else:
                print("Something went wrong!")
                exit()

    


def picklist_to_activity_object(project_template_df, record_id):
    records = []
    for index, row in project_template_df.iterrows():
        if row["Type"] == "Picklist":
             record = {
                "Name": str(row['Activity Name']),
                "sitetracker__Project_Template__c": record_id,
                "sitetracker__Activity_Section__c": str(row['Sections']),
                "sitetracker__Activity_Type__c": "Field",
                "sitetracker__Actualize_on_Checklist_Completion__c": False,
                "sitetracker__Actualize_on_Document_Upload__c": False,
                "sitetracker__Amount__c": 0,
                "sitetracker__Document_Uploads__c": "Allow Document Uploads",
                "sitetracker__Email_on_Activity_Start__c": False,
                "sitetracker__Enable_Scheduling__c": False,
                "sitetracker__Field_Name__c": str(row['Activity Name']).replace(" ", "_") + "_Picklist__c",
                "sitetracker__Financial_Activity__c": False,
                "sitetracker__Hide_Forecast__c": False,
                "sitetracker__Optional_Activity__c": False,
                "sitetracker__Order__c": str(row['Order']),
                "sitetracker__Project_Milestone__c": "a0kC3000000jWVBIA2",
                "sitetracker__Propagate_Name_Changes__c": False,
                "sitetracker__Require_Completed_Checklist_to_Actualize__c": False,
                "sitetracker__Schedule_Using_Predecessor_Forecast_Date__c": False,
                "sitetracker__Schedule_Width__c": "Small",
                "sitetracker__Sub_Milestone__c": False,
                "Requires_Approval__c": False,
                "Auto_Submit_for_Approval__c": False,
                "sitetracker__Actualize_on_Job_Completion__c": False,
                "sitetracker__Require_Job_Completion_to_Actualize__c": False,
                "sitetracker__Critical__c": False,
                "sitetracker__Enable_Promise_Date__c": False,
            }
             records.append(record)
    return records

def milestone_to_activity_object(project_template_df, project_milestones, record_id):

    milestone_ids = []
    records = []

    for index, row in project_template_df.iterrows():
        for milestone in project_milestones:
            if row["Type"] == "Milestone" and milestone["Name"] == row["Activity Name"]:
                milestone_ids.append(milestone["Id"])

    print(milestone_ids)
    milestone_index = 0
    for index, row in project_template_df.iterrows():
        if row["Type"] == "Milestone" :
             record = {
                "Name": str(row['Activity Name']),
                "sitetracker__Project_Template__c": str(record_id),
                "sitetracker__Activity_Section__c": str(row['Sections']),
                "sitetracker__Activity_Type__c": "Milestone",
                "sitetracker__Actualize_on_Checklist_Completion__c": False,
                "sitetracker__Actualize_on_Document_Upload__c": False,
                "sitetracker__Days_Type__c": "Business",
                "sitetracker__Document_Uploads__c": "Allow Document Uploads",
                "sitetracker__Email_on_Activity_Start__c": False,
                "sitetracker__Enable_Scheduling__c": False,
                "sitetracker__Financial_Activity__c": False,
                "sitetracker__Hide_Forecast__c": False,
                "sitetracker__Optional_Activity__c": False,
                "sitetracker__Order__c": str(row['Order']),
                "sitetracker__Project_Milestone__c": milestone_ids[milestone_index],
                "sitetracker__Propagate_Name_Changes__c": False,
                "sitetracker__Require_Completed_Checklist_to_Actualize__c": False,
                "sitetracker__Schedule_Using_Predecessor_Forecast_Date__c": False,
                "sitetracker__Sub_Milestone__c": False,
                "sitetracker__WBS_Code__c": str(row['WBS_Code']),
                "Requires_Approval__c": False,
                "Auto_Submit_for_Approval__c": False,
                "sitetracker__Actualize_on_Job_Completion__c": False,
                "sitetracker__Require_Job_Completion_to_Actualize__c": False,
                "sitetracker__Critical__c": False,
                "sitetracker__Enable_Promise_Date__c": False,
            }
             milestone_index += 1
             records.append(record)
    return records

def insert_milestone_records_to_activity_object(records, bulk):
    try:
        # print("Milestone Records to Activity Object:")
        # print(records)
        job = bulk.create_insert_job("sitetracker__Activity_Template__c", contentType='CSV')
        csv_iter = CsvDictsAdapter(iter(records))
        batch = bulk.post_batch(job, csv_iter)
        bulk.wait_for_batch(job, batch)
        bulk.close_job(job)
        result = bulk.get_batch_results(batch)
        print("insert_milestone_records_to_activity_object: ")
        print(result)
    except SalesforceMalformedRequest as e:
            print(type(e.content))
            print(e.content)
            if str(e.content[0]).find("There is already a field named") != -1:
               pass
            else:
                print("Something went wrong!")
                exit()


def insert_picklist_records_to_activity_object(records, bulk):

    try:
        # print("Picklist Records to Activity Object:")
        # print(records)
        job = bulk.create_insert_job("sitetracker__Activity_Template__c", contentType='CSV')
        csv_iter = CsvDictsAdapter(iter(records))
        batch = bulk.post_batch(job, csv_iter)
        bulk.wait_for_batch(job, batch)
        bulk.close_job(job)
        result = bulk.get_batch_results(batch)
        print("insert_picklist_records_to_activity_object: ")
        print(result)
    except SalesforceMalformedRequest as e:
            print(type(e.content))
            print(e.content)
            if str(e.content[0]).find("There is already a field named") != -1:
               pass
            else:
                print("Something went wrong!")
                exit()

def upsert_data_into_project_template_version(record_id, session_id, instance, sf):

    query = f"Select Id from sitetracker__Project_Template_Version__c  where sitetracker__Project_Template__r.Id='{record_id}'"
    data = sf.query(query)
    # print(data)
    data_str = str(data)
    startIndex = str(data).find("Id") + 6
    endIndex = startIndex + 18
    project_template_version_record_id = data_str[startIndex:endIndex]
    # print("project_template_version_record_id: ")
    # print(project_template_version_record_id)

    record_data = {
        "sitetracker__Status__c": "Active"
    }

    # Upsert the record
    sf_project_template_version_object = SFType('sitetracker__Project_Template_Version__c', session_id, instance)
    sf_project_template_version_object.upsert(project_template_version_record_id, record_data)

    
def update_project_tempate_record_final(record_id, session_id, instance, sf):

    name = get_project_template_csv_name()

    record_data = {
        "sitetracker__Active__c": True,
        "sitetracker__Complete_Status__c": "Complete",
        "Name": str(name)  + "_" + str(random.randint(0, 1000)) + "_Final"
    }

    # Upsert the record
    sf_project_template_version_object = SFType('sitetracker__Project_Template__c', session_id, instance)
    sf_project_template_version_object.upsert(record_id, record_data)


session_id, instance, sf = connect_to_salesforce()
bulk = SalesforceBulk(sessionId=session_id, host=instance)

record_id = insert_project_template_record(session_id, instance, sf)

file_df = read_project_template_name_csv()

fields, records = get_fields_to_create_for_project_object(project_template_file=file_df, sf=sf)

create_custom_fields_for_project_object(fields=fields, sf=sf)

project_milestones = insert_records_into_project_milestone_object(records=records, bulk=bulk)

milestone_records = milestone_to_activity_object(project_milestones= project_milestones, project_template_df=file_df, record_id=record_id)

picklist_records = picklist_to_activity_object(project_template_df=file_df, record_id=record_id)

insert_milestone_records_to_activity_object(records=milestone_records, bulk=bulk)

insert_picklist_records_to_activity_object(records=picklist_records, bulk=bulk)

upsert_data_into_project_template_version(record_id=record_id, instance=instance, session_id=session_id, sf=sf)

# update_project_tempate_record_final(record_id=record_id, instance=instance, session_id=session_id, sf=sf)







