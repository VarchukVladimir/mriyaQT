__author__ = 'Volodymyr Varchuk'
__email__ = "vladimir.varchuk@rackspace.com"


import logging
from project_utils import Timer
from json import load, dump
import csvquerytool
import re


WF_EXECUTE = 'execute'
WF_MAPPING = 'mapping'

JT_QUERY = 'query'
JT_INSERT = 'insert'
JT_UPDATE = 'update'
JT_UPSERT = 'upsert'
JT_DELETE = 'delete'

class BatchDistributor:
    def __init__(self, ids_list, split_size, max_threads):
        self.ids_list = sorted(ids_list)
        self.split_size = split_size
        self.max_threads = max_threads

    def distribute_ids(self):
        record_count = 0
        split_list=[]
        for record_id in self.ids_list:
            if record_count == 0:
                begein_interval = str(record_id)
            record_count = record_count + 1
            if record_count == self.split_size:
                split_list.append('{0}-{1}'.format(begein_interval, str(record_id)))
                record_count = 0
        return split_list


class MigrationWorkflow:
    def __init__(self, src, dst, workfow):
        self.src = src
        self.dst = dst
        self.workflow = workfow


    def execute_sf_job(self, job_params):
        job_message = job_params['message']
        job_type = job_params['command']
        logging.info('Input data {}'.format(job_params['input_data']))

        if job_type != JT_QUERY:
            print('Input data {}'.format(job_params['input_data']))
            input_data = open(job_params['input_data'])
        else:
            print('Input data {}'.format(re.sub(' +', ' ',job_params['input_data']).strip()))
            input_data = job_params['input_data']
        output_data = job_params['output_data']
        logging.info('Output data {}'.format(output_data))
        print('Output data {}'.format(output_data))
        object = job_params['object']
        if 'exteranl_id_name' in job_params.keys():
            external_id_name = job_params['exteranl_id_name']
        else:
            external_id_name = None
        if type(job_params['connector']) is str or job_params['connector'] is unicode:
            connector = self.src if job_params['connector'] == 'src' else self.dst
        else:
            connector = job_params['connector']

        print('\t'+job_message)
        logging.info(job_message)
        if job_type == JT_QUERY:
            res = connector.bulk_load ( object, input_data, None, output_data)
            rf = open(output_data,'r')
            inside = rf.readline()
            rf.close()
            if '"Records not found for this query"' in inside:
                with open(output_data, 'w') as f:
                    res_re = re.search('SELECT(.*?)FROM',input_data, re.IGNORECASE).group(1).strip().split(',')
                    columns = [ column.strip() for column in res_re]
                    w_str = ','.join(['"{0}"'.format(column) for column in columns])
                    f.write(w_str)
        if job_type == JT_UPSERT:
            external_id_name = job_params['exteranl_id_name'][0]
            res = connector.bulk_upsert(object, external_id_name, input_data)
        if job_type == JT_INSERT:
            res = connector.bulk_insert(object, input_data, external_id_name)
        if job_type == JT_UPDATE:
            res = connector.bulk_update(object, input_data, external_id_name)
        if job_type == JT_DELETE:
            res = connector.bulk_delete(object, job_params['where_condition'], external_id_name)
        if res:
            dump(res,open(output_data,'w'))


        return job_message

    def execute_sql_query(self, sql_params):
        job_message = sql_params['message']

        sql = re.sub(' +', ' ',sql_params['sql']).strip()

        # sql = sql_params['sql']
        input_data = sql_params['input_data']
        output_data = sql_params['output_data']
        logging.info('input files {0}'.format( ' '.join(input_data)))
        logging.info('output files {0}'.format(output_data))
        logging.info(job_message)
        print('\t input files {0}'.format( ' '.join(input_data)))
        print('\t SQL {0}'.format( sql))
        print('\t output files {0}'.format(output_data))
        print('\t'+job_message)
        if type(output_data) == str:
            output_data = open(output_data, 'w')
        csvquerytool.run_query(sql,input_data,output_data)
        return job_message


    def execute_mapping(self, mapping_params):
        job_message = mapping_params['message']
        mapping_info = mapping_params['mapping_info']
        input_data = mapping_params['input_data']
        output_data = mapping_params['output_data']
        print('\t'+job_message)
        sql = mapping_info.get_sql_exchanged_column_names(mapping_params['operation'])
        print('input: {}'.format(input_data))
        print(sql)
        print('output: {}'.format(output_data))
        logging.info(job_message)
        logging.info('Input data {}'.format(input_data))
        logging.info('Output data {}'.format(output_data))
        logging.info(sql)
        csvquerytool.run_query(sql,input_data,open(output_data, 'w'))
        return job_message


    def execute_workflow(self):
        ttimer = Timer()
        for workflow_step in self.workflow:
            ttimer.start()
            wf_type = workflow_step.iterkeys().next()
            print(wf_type)
            logging.info(wf_type)
            if wf_type == WF_EXECUTE:
                res = self.execute_sf_job(workflow_step[wf_type])
            elif wf_type == WF_MAPPING:
                res = self.execute_mapping(workflow_step[wf_type])
            else:
                res = self.execute_sql_query(workflow_step[wf_type])
            print('\t'+res + ' [DONE] time elapsed - [{0}]'.format(ttimer.stop()))
            logging.info(res + ' [DONE] time elapsed - [{0}]'.format(ttimer.stop()))
