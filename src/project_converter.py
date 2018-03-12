__author__ = 'Volodymyr Varchuk'
__email__ = 'vladimir.varchuk@rackspace.com'

'''
Mriya Query Tool
================

Application for extracting and querying SalesForce data using SQL (SQLite engine)

'''


from sys import argv
from os import path as p
import json

if len(argv) == 2:
    project_file_name = argv[1]
    if p.exists(project_file_name):
        project_data = json.load(open(project_file_name))
        for wf_item in project_data['workflow']:
            if wf_item['type'] == 'SF_Execute' and wf_item['command'] == 'insert':
                wf_item['external_id_name'] = 'AcquisitionId__c'
            elif wf_item['type'] == 'SF_Execute' and wf_item['command'] == 'update':
                wf_item['external_id_name'] = 'Id'
        json.dump(project_data, open(project_file_name, 'w'))
else:
    print ( 'You must specify one project file to convert')
