import pandas as pd
from pymongo import MongoClient

"""Establishing connection """ 

cluster = MongoClient('Connection string goes here')

db = cluster['h1bdb']

employee_details_collection = db['employee_details']
agent_details_collection = db['agent_details']
employee_office_details_collection = db['employee_office_details']
employee_wage_details_collection = db['employee_wage_details']
case_details_collection = db['case_details']
employer_details_collection = db['employer_details']
employee_change_details_collection = db['employee_change_details']  

print("Connection Established successfully.")

""" Inserting data into collections """

# employee_details_df = pd.read_csv('E:\MSDA\Sem 1\DBMS\Project\Data\Individual/Employee_Details_22_Min.csv')
# employee_details_Dict = employee_details_df.to_dict('records')

# agent_details_df = pd.read_csv('E:\MSDA\Sem 1\DBMS\Project\Data\Individual/Agent_Details_22_Min.csv')
# agent_details_Dict = agent_details_df.to_dict('records')

# case_details_df = pd.read_csv('E:\MSDA\Sem 1\DBMS\Project\Data\Individual/Case_Details_22_Min.csv')
# case_details_Dict = case_details_df.to_dict('records')

# employee_change_details_df = pd.read_csv('E:\MSDA\Sem 1\DBMS\Project\Data\Individual/Employee_Change_Details_22_Min.csv')
# employee_change_details_Dict = employee_change_details_df.to_dict('records')

# employee_office_details_df = pd.read_csv('E:\MSDA\Sem 1\DBMS\Project\Data\Individual/Employee_Office_Details_22_Min.csv')
# employee_office_details_Dict = employee_office_details_df.to_dict('records')

# employee_wage_details_df = pd.read_csv('E:\MSDA\Sem 1\DBMS\Project\Data\Individual/Employee_Wage_Details_22_Min.csv')
# employee_wage_details_Dict = employee_wage_details_df.to_dict('records')

# employer_details_df = pd.read_csv('E:\MSDA\Sem 1\DBMS\Project\Data\Individual/Employer_Details_22_Min.csv')
# employer_details_Dict = employer_details_df.to_dict('records')

# employee_details_collection.insert_many(employee_details_Dict)
# agent_details_collection.insert_many(agent_details_Dict)
# case_details_collection.insert_many(case_details_Dict)
# employee_change_details_collection.insert_many(employee_change_details_Dict)
# employee_office_details_collection.insert_many(employee_office_details_Dict)
# employee_wage_details_collection.insert_many(employee_wage_details_Dict)
# employer_details_collection.insert_many(employer_details_Dict)

# print('Data inserted successfully.')

""" Reading from collection """

# results = employee_details_collection.find({})

# for x in results:
#     print(x)


""" Updating value in collection """

# updatequery = { 'Employee_Id': 10003, 'JOB_TITLE': 'Data Analyst', 'FULL_TIME_POSITION': 'Y', 'Employer_Id': 102 }
# newVlaues = { "$set": { 'JOB_TITLE': 'DATA SCIENTIST' } }

# update = employee_details_collection.update_many(updatequery, newVlaues)

# print(update.modified_count, "documents updated.")


""" Deleting collections """

""" Deleting records in collection with condition """

# deletequery = { 'Employee_Id': 322986 }

# delete = employee_details_collection.delete_many(deletequery)

# print(delete.deleted_count, " documents deleted.")

""" Deleting entire records in collection """

# delete1 = employee_details_collection.delete_many({})
# delete2 = agent_details_collection.delete_many({})
# delete3 = case_details_collection.delete_many({})
# delete4 = employee_change_details_collection.delete_many({})
# delete5 = employee_wage_details_collection.delete_many({})
# delete6 = employer_details_collection.delete_many({})
# delete7 = employee_office_details_collection.delete_many({})


""" Aggregate pipelines using python """

""" 1. Top 10 agents in 2022 who got visa approved """

# pipeline1 = [
#     {
#         '$lookup': {
#             'from': 'employee_details', 
#             'localField': 'Employee_Id', 
#             'foreignField': 'Employee_Id', 
#             'as': 'employee_details'
#         }
#     }, {
#         '$unwind': '$employee_details'
#     }, {
#         '$lookup': {
#             'from': 'employer_details', 
#             'localField': 'employee_details.Employer_Id', 
#             'foreignField': 'Employer_Id', 
#             'as': 'employer_details'
#         }
#     }, {
#         '$unwind': '$employer_details'
#     }, {
#         '$lookup': {
#             'from': 'agent_details', 
#             'localField': 'employee_details.Employee_Id', 
#             'foreignField': 'Employee_Id', 
#             'as': 'agent_details'
#         }
#     }, {
#         '$unwind': '$agent_details'
#     }, {
#         '$match': {
#             '$or': [
#                 {
#                     'CASE_STATUS': 'Certified'
#                 }, {
#                     'CASE_STATUS': 'CERTIFIED'
#                 }
#             ], 
#             'VISA_CLASS': 'H-1B', 
#             'agent_details.AGENT_ATTORNEY_NAME': {
#                 '$ne': 'No Agent'
#             }
#         }
#     }, {
#         '$group': {
#             '_id': '$agent_details.AGENT_ATTORNEY_NAME', 
#             'count': {
#                 '$sum': 1
#             }
#         }
#     },{
#         '$sort': {
#             'count': -1
#         }
#     }, {
#         '$limit': 10
#     }, {
#         '$project': {
#             '_id': 0, 
#             'Agent_Attorney_Name': '$_id', 
#             'total_employers_approved': '$count'
#         }
#     }
# ]

# results1 = case_details_collection.aggregate(pipeline1)

# for result in results1:
#     print(result)


""" 2. Top 10 job title in 2022 having the highest chances of getting visa denied """

# pipeline2 = [
#     {
#         '$lookup': {
#             'from': 'employee_wage_details', 
#             'localField': 'Employee_Id', 
#             'foreignField': 'Employee_Id', 
#             'as': 'employee_wage_details'
#         }
#     }, {
#         '$unwind': '$employee_wage_details'
#     }, {
#         '$lookup': {
#             'from': 'case_details', 
#             'localField': 'Employee_Id', 
#             'foreignField': 'Employee_Id', 
#             'as': 'case_details'
#         }
#     }, {
#         '$match': {
#             '$or': [
#                 {
#                     'case_details.CASE_STATUS': 'Denied'
#                 }, {
#                     'case_details.CASE_STATUS': 'DENIED'
#                 }
#             ], 
#             'case_details.VISA_CLASS': 'H-1B'
#         }
#     }, {
#         '$group': {
#             '_id': '$JOB_TITLE', 
#             'count': {
#                 '$sum': 1
#             }
#         }
#     }, {
#         '$sort': {
#             'count': -1
#         }
#     }, {
#         '$limit': 10
#     }, {
#         '$project': {
#             '_id': 0, 
#             'Job_Title': '$_id', 
#             'Total_Cases': '$count'
#         }
#     }
# ]

# results2 = employee_details_collection.aggregate(pipeline2)

# for result in results2:
#     print(result)


""" 3. Top 10 employer in 2022 who has the highest number of withdrawn cases """

# pipeline3 = [
#     {
#         '$match': {
#             '$or': [
#                 {
#                     'CASE_STATUS': 'Withdrawn'
#                 }, {
#                     'CASE_STATUS': 'WITHDRAWN'
#                 }
#             ], 
#             'VISA_CLASS': 'H-1B'
#         }
#     }, {
#         '$lookup': {
#             'from': 'employee_details', 
#             'localField': 'Employee_Id', 
#             'foreignField': 'Employee_Id', 
#             'as': 'employee_details'
#         }
#     }, {
#         '$unwind': '$employee_details'
#     }, {
#         '$lookup': {
#             'from': 'employer_details', 
#             'localField': 'employee_details.Employer_Id', 
#             'foreignField': 'Employer_Id', 
#             'as': 'employer_details'
#         }
#     }, {
#         '$unwind': '$employer_details'
#     }, {
#         '$group': {
#             '_id': '$employer_details.EMPLOYER_NAME', 
#             'count': {
#                 '$sum': 1
#             }
#         }
#     }, {
#         '$sort': {
#             'count': -1
#         }
#     }, {
#         '$limit': 10
#     }, {
#         '$project': {
#             '_id': 0, 
#             'employer_name': '$_id', 
#             'total_cases': '$count'
#         }
#     }
# ]

# results3 = case_details_collection.aggregate(pipeline3)

# for result in results3:
#     print(result)


""" 4. Top 10 companies in 2022 that has highest number of changed employer number who got their visa approved """

# pipeline4 = [
#     {
#         '$lookup': {
#             'from': 'employee_details', 
#             'localField': 'Employer_Id', 
#             'foreignField': 'Employer_Id', 
#             'as': 'employee_details'
#         }
#     }, {
#         '$unwind': '$employee_details'
#     },  {
#         '$lookup': {
#             'from': 'employee_change_details', 
#             'localField': 'employee_details.Employee_Id', 
#             'foreignField': 'Employee_Id', 
#             'as': 'employee_change_details'
#         }
#     }, {
#         '$unwind': '$employee_change_details'
#     }, {
#         '$lookup': {
#             'from': 'case_details', 
#             'localField': 'employee_details.Employee_Id', 
#             'foreignField': 'Employee_Id', 
#             'as': 'case_details'
#         }
#     }, {
#         '$match': {
#             '$or': [
#                 {
#                     'case_details.CASE_STATUS': 'Certified'
#                 }, {
#                     'case_details.CASE_STATUS': 'CERTIFIED'
#                 }
#             ], 
#             'case_details.VISA_CLASS': 'H-1B'
#         }
#     }, {
#         '$group': {
#             '_id': '$EMPLOYER_NAME', 
#             'count': {
#                 '$sum': '$employee_change_details.CHANGE_EMPLOYER'
#             }
#         }
#     }, {
#         '$sort': {
#             'count': -1
#         }
#     }, {
#         '$limit': 10
#     }, {
#         '$project': {
#             '_id': 0, 
#             'employer_name': '$_id', 
#             'total_changed_employer': '$count'
#         }
#     }
# ]

# results4 = employer_details_collection.aggregate(pipeline4)

# for result in results4:
#     print(result)


""" 5. Top 10 employer in 2022 has visa approved and provides highest yearly salary """

# pipeline5 = [
#     {
#         '$lookup': {
#             'from': 'employee_details', 
#             'localField': 'Employer_Id', 
#             'foreignField': 'Employer_Id', 
#             'as': 'employee_details'
#         }
#     }, {
#         '$unwind': '$employee_details'
#     }, {
#         '$lookup': {
#             'from': 'employee_wage_details', 
#             'localField': 'employee_details.Employee_Id', 
#             'foreignField': 'Employee_Id', 
#             'as': 'employee_wage_details'
#         }
#     }, {
#         '$unwind': '$employee_wage_details'
#     }, {
#         '$lookup': {
#             'from': 'case_details', 
#             'localField': 'employee_details.Employee_Id', 
#             'foreignField': 'Employee_Id', 
#             'as': 'case_details'
#         }
#     }, {
#         '$unwind': '$case_details'
#     }, {
#         '$match': {
#             '$or': [
#                 {
#                     'case_details.CASE_STATUS': 'Certified'
#                 }, {
#                     'case_details.CASE_STATUS': 'CERTIFIED'
#                 }
#             ], 
#             'case_details.VISA_CLASS': 'H-1B', 
#             'employee_wage_details.WAGE_UNIT_OF_PAY': 'Year'
#         }
#     }, {
#         '$group': {
#             '_id': '$Employer_Id',
#             'name': {'$first': '$EMPLOYER_NAME'},
#             'count': {
#                 '$sum': '$employee_wage_details.WAGE_RATE_OF_PAY_FROM'
#                 }
#         }
#     }, {
#         '$sort': {
#             'count': -1
#         }
#     }, {
#         '$limit': 10
#     }, {
#         '$project': {
#             '_id':0,
#             'employer_name': '$name', 
#             'total_yearly_salary': '$count'
#         }
#     }
# ]

# results5 = employer_details_collection.aggregate(pipeline5)

# for result in results5:
#     print(result)

""" 6. Top 10 job titles where employees have hourly paya and has the highest number of cases approved."""

# pipeline6 = [
#     {
#         '$lookup': {
#             'from': 'employee_wage_details', 
#             'localField': 'Employee_Id', 
#             'foreignField': 'Employee_Id', 
#             'as': 'employee_wage_details'
#         }
#     }, {
#         '$unwind': '$employee_wage_details'
#     }, {
#         '$lookup': {
#             'from': 'case_details', 
#             'localField': 'Employee_Id', 
#             'foreignField': 'Employee_Id', 
#             'as': 'case_details'
#         }
#     }, {
#         '$unwind': '$case_details'
#     }, {
#         '$match': {
#             '$or': [
#                 {
#                     'case_details.CASE_STATUS': 'Certified'
#                 }, {
#                     'case_details.CASE_STATUS': 'CERTIFIED'
#                 }
#             ], 
#             'case_details.VISA_CLASS': 'H-1B', 
#             'employee_wage_details.WAGE_UNIT_OF_PAY': 'Hour', 
#             'FULL_TIME_POSITION': 'N'
#         }
#     }, {
#         '$group': {
#             '_id': '$JOB_TITLE', 
#             'total_wage': {
#                 '$sum': '$employee_wage_details.WAGE_RATE_OF_PAY_FROM'
#             }
#         }
#     }, {
#         '$sort': {
#             'total_wage': -1
#         }
#     }, {
#         '$limit': 10
#     }, {
#         '$project': {
#             '_id': 0, 
#             'job_title': '$_id', 
#             'total_yearly_salary': '$total_wage'
#         }
#     }
# ]

# results6 = employee_details_collection.aggregate(pipeline6)

# for result in results6:
#    print(result)