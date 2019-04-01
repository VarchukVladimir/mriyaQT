from time import sleep
from urlparse import urlparse

__author__ = 'Volodymyr Varchuk'
__email__ = "vladimir.varchuk@rackspace.com"

import beatbox
import logging
import sys
import csv
from project_utils import spin, from_csv_to_dict, printProgress
from collections import namedtuple
from salesforce_bulk__ import *
import json
from json import loads, load, dump, dumps
import StringIO
from multiprocessing import Pool, Queue
import sqlparse
import requests
from sf_merge import SoapMerge
from httplib2 import Http

import pymssql
from collections import namedtuple
import csv

ConnectorParam = namedtuple('ConnectorParam',
                            ['username', 'password', 'url_prefix',
                             'organization_id', 'consumer_key',
                             'consumer_secret', 'token', 'threads', 'security_token'])
UploaderParam = namedtuple('UploaderParam',
                           ['job', 'header', 'batch_data', 'batch_number'])
MultithreadLoadParam = namedtuple('MultithreadLoadParam',
                                  ['object_name', 'soql', 'header_columns', 'csv_file', 'condition'])
MSSQLConnectorParam = namedtuple('MSSQLConnectorParam',
                                 ['server', 'user', 'password', 'database'])

QUERY_LIMIT = 200
BATCH_SIZE = 200
BATCH_SIZE_LIMIT = 9 * 1024 * 1024
session_file = 'sessions.ini'
DS_TYPE_SF = 'SALESFORCE'
DS_TYPE_JSON = 'JSON'
DS_TYPE_CSV = 'CSV'
BULK_BATCH_SIZE = 10000
BULK_CONCURRENCY_MODE = 'Parallel'

SOAP_MERGE_REQUEST_HEADERS = {
    'content-type': 'text/xml',
    'charset': 'UTF-8',
    'SOAPAction': 'merge'
}


def get_conn_param(conf_dict):
    if 'type' in conf_dict.keys():
        if conf_dict['type'] == 'mssql':
            param = MSSQLConnectorParam(conf_dict['server'].encode('utf-8'),
                                        conf_dict['user'].encode('utf-8'),
                                        conf_dict['password'].encode('utf-8'),
                                        '')
            return param
    param = ConnectorParam(conf_dict['username'].encode('utf-8'),
                           conf_dict['password'].encode('utf-8'),
                           conf_dict['host_prefix'].encode('utf-8'),
                           '',
                           conf_dict['consumer_key'].encode('utf-8'),
                           conf_dict['consumer_secret'].encode('utf-8'),
                           '',
                           int(conf_dict['threads'].encode('utf-8')),
                           conf_dict['security_token'].encode('utf-8'))
    return param


class MsSQLConnector:
    def __init__(self, connection_params):
        self.connection = pymssql.connect(server=connection_params.server, database=connection_params.database,
                                          user=connection_params.user,
                                          password=connection_params.password)

    def query(self, sql, output_file):
        self.cursor = self.connection.cursor()
        self.cursor.execute(sql)
        f = csv.writer(file(output_file, 'wb'))
        f.writerow([d[0] for d in self.cursor.description])
        rows_to_write = []
        row = self.cursor.fetchone()
        row_count = 0
        while row:
            rows_to_write.append([unicode(column).encode('utf-8') for column in row])
            row_count = row_count + 1
            if row_count % 10000 == 0:
                f.writerows(rows_to_write)
                rows_to_write = []
                # print('fetched {0} out of {1}'.format(row_count, self.cursor.rowcount))
            row = self.cursor.fetchone()
        f.writerows(rows_to_write)
        print('total row count {0}'.format(row_count))


class SalesforceBulkExtended(SalesforceBulk):

    def __init__(self, **kwargs):
        super(SalesforceBulkExtended, self).__init__(**kwargs)
        self.restendpoint = self.endpoint[:self.endpoint.find('/services/async/')] + '/services/data/v42.0/'

    def rest_request(self, url_reuqest=''):
        http = Http()
        url = self.restendpoint + url_reuqest
        'Authorization: Bearer '
        headers = {"Authorization": "Bearer " + self.sessionId,
                   "Content-Type": "application/json; charset=UTF-8"}
        resp, content = http.request(url, "GET", headers=headers)
        self.check_status(resp, content)
        if resp['status'] == '200':
            return content
        return None


class SFBeatboxConnector:
    def __init__(self, connector_param, batch_size=1000):
        self.svc = beatbox.PythonClient()
        if (not self.svc):
            print('no connection')
            return
        self.batch_size = batch_size
        logging.info('Process created for batch size {0} '.format(batch_size))
        if connector_param.url_prefix != '':
            self.svc.serverUrl = self.svc.serverUrl.replace('login.', connector_param.url_prefix)
        try:
            self.login_result = self.svc.login(connector_param.username, connector_param.password)
            logging.info('Connected to instance {0}'.format(self.svc.serverUrl))
            print('Connected to instance {0}'.format(self.svc.serverUrl))
            self.connected = True
        except:
            self.connected = False
            print(sys.exc_info()[1])

    def fetch_all_data(self, soql):
        query_result = self.svc.query(soql)
        rows = query_result['records']
        total_records = query_result['size']
        query_locator = query_result['queryLocator']
        ret = printProgress(len(rows), total_records, prefix='Data loaded', suffix='Complete', decimals=1,
                            barLength=100)
        while query_result['done'] is False and len(rows) < total_records:
            query_result = self.svc.queryMore(query_locator)
            query_locator = query_result['queryLocator']
            rows = rows + query_result['records']
            ret = printProgress(len(rows), total_records, prefix='Data loaded', suffix='Complete', decimals=1,
                                barLength=100)
        if ret == 1:
            printProgress(total_records, total_records, prefix='Data loaded', suffix='Complete', decimals=1,
                          barLength=100)
        return rows

    def write_batch_tocsv(self, csv_name, batch_data, headers, write_header=False):
        if write_header:
            open_mode = "wb"
        else:
            open_mode = "ab"
        with open(csv_name, open_mode) as f_csv:
            writer = csv.DictWriter(f_csv, fieldnames=headers)
            if write_header:
                writer.writeheader()
            for row in batch_data:
                writer.writerow(row)
        return len(batch_data)

    def export_to_csv(self, soql, file_to_export):
        query_result = self.svc.query(soql)
        records_buff = query_result['records']
        total_records = query_result['size']
        query_locator = query_result['queryLocator']
        headers = records_buff[0].keys()
        total_wrote = 0
        write_header = True
        ret = printProgress(total_wrote, total_records, prefix='Data loaded', suffix='Complete', decimals=1,
                            barLength=100)
        while total_wrote < total_records:
            if len(records_buff) >= self.batch_size or query_result['done'] is True:
                total_wrote = total_wrote + self.write_batch_tocsv(file_to_export, records_buff, headers,
                                                                   write_header=write_header)
                ret = printProgress(total_wrote, total_records, prefix='Data loaded', suffix='Complete', decimals=1,
                                    barLength=100)
                if write_header:
                    write_header = False
                records_buff = []
                logging.info('Batch wrote {0} records'.format(total_wrote))
                if query_result['done'] is True:
                    break
            query_result = self.svc.queryMore(query_locator)
            query_locator = query_result['queryLocator']
            records_buff = records_buff + query_result['records']
        if ret == 1:
            printProgress(total_records, total_records, prefix='Data loaded', suffix='Complete', decimals=1,
                          barLength=100)
        print('\n')
        return total_wrote

    def update(self, data):
        result = self.svc.update(data)
        logging.info(result)
        return result

    def chunked_create(self, raw_data):
        result = []
        if len(raw_data) > QUERY_LIMIT or len(dumps(raw_data)) >= 52428800:
            data_chunk = []
            for row in raw_data:
                packet_size = len(dumps(data_chunk)) + len(dumps(row)) + 100
                if len(data_chunk) == QUERY_LIMIT or packet_size >= 52428800:
                    result = result + self.svc.create(data_chunk)
                    data_chunk = []
                data_chunk.append(row)
            result = result + self.svc.create(data_chunk)
        else:
            result = self.svc.create(raw_data)
        return result

    def chunked_delete(self, raw_data):
        result = []
        if len(raw_data) > QUERY_LIMIT:
            data_chunk = []
            for row in raw_data:
                if len(data_chunk) == QUERY_LIMIT:
                    result = result + self.svc.delete(data_chunk)
                    data_chunk = []
                data_chunk.append(row)
            result = result + self.svc.delete(data_chunk)
        else:
            result = self.svc.delete(raw_data)
        return result

    def chunked_update(self, raw_data):
        result = []
        if len(raw_data) > QUERY_LIMIT:
            data_chunk = []
            for row in raw_data:
                if len(data_chunk) == QUERY_LIMIT:
                    result = result + self.svc.update(data_chunk)
                    data_chunk = []
                data_chunk.append(row)
            result = result + self.svc.update(data_chunk)
        else:
            result = self.svc.update(raw_data)
        return result

    def merge(self, object_name, input_data):
        """
        data should be:
        MasterId,MergedId
        000001,000002
        000001,000003
        000001,000004
        000005,000006
        000005,000007
        """

        if type(input_data) == str:
            raw_data = from_csv_to_dict(input_data)
        else:
            raw_data = input_data

        # check if there are no merged ids in master ids list
        for i, master_row in enumerate(raw_data):
            merged_count = 0
            for j, merge_row in enumerate(raw_data):
                if master_row['MasterId'] == merge_row['MergedId']:
                    print(
                        "It is impossible to perform merge operation on this data set. MasterId {} apears as MergedId".format(
                            master_row['MasterId']))
                    return -1
                if master_row['MergedId'] == merge_row['MergedId'] and i <> j:
                    print(
                        "It is impossible to perform merge operation on this data set. MergedId {} apears in two different rows".format(
                            master_row['MergedId']))
                    return -1

        # convert to format [{'master_id':['merged_id1', 'merged_id2', 'merged_id3']}]
        # calculate how many attepts needed to complete task
        data = {}
        max_merged_count = 0
        for master_row in raw_data:
            if master_row['MasterId'] not in data.keys():
                merge_ids = []
                for merge_row in raw_data:
                    if master_row['MasterId'] == merge_row['MasterId']:
                        merge_ids.append(merge_row['MergedId'])
                if len(merge_ids) > max_merged_count:
                    max_merged_count = len(merge_ids)
                data[master_row['MasterId']] = merge_ids
        print(data)

        max_merged_ids = max([len(merged_ids)] for merged_ids in data.itervalues())[0]

        save_max_merged_ids = max_merged_ids
        batch = {}
        while len(data) > 0:
            while max_merged_ids > 0:
                for master_id in data.keys():
                    merge_ids = data[master_id]
                    if len(merge_ids) == max_merged_ids:
                        if master_id not in batch.keys() and BATCH_SIZE > len(batch):
                            batch[master_id] = merge_ids[:2]
                            del merge_ids[:2]
                            if len(merge_ids) == 0:
                                del data[master_id]
                        elif BATCH_SIZE <= len(batch):
                            max_merged_ids -= 1
                            self._run_merge(batch_data=batch, object_name=object_name)
                            batch = {}
                            max_merged_ids = save_max_merged_ids
                max_merged_ids -= 1
            self._run_merge(batch_data=batch, object_name=object_name)
            batch = {}
            max_merged_ids = save_max_merged_ids

    def _count_dups(self, data):
        pass

    def _run_merge(self, batch_data, object_name):
        soap_merge = SoapMerge(instance_url=self.svc.serverUrl, sessionid=self.svc.sessionId)
        merge_result = soap_merge.merge(object_name, batch_data)
        return merge_result

    def _result(self, res):
        if 'mergeResponse' in res:
            return res['mergeResponse']
        else:
            return [res]


class DictAdapter:
    def __init__(self, headers=None, first_row_is_header=True):
        self.first_row_is_header = first_row_is_header
        self.headers = headers

    def get_dict(self, data):
        dict_data = []
        if self.headers is None and self.first_row_is_header:
            self.headers = data[0]
        else:
            return None
        for row in data[1:] if self.first_row_is_header else data:
            row_dict = {}
            for i, caption in enumerate(self.headers):
                row_dict[caption] = row[i]
            dict_data.append(row_dict)
        return dict_data

    def get_list(self, data):
        list_data = []
        if self.headers is None:
            self.headers = data[0].keys()
        else:
            return None
        list_data.append(self.headers)
        for row in data:
            row_list = []
            for caption in self.headers:
                row_list.append(row[caption])
            list_data.append(row_list)
        return list_data


class RESTConnector:
    def __init__(self, connector_param, batch_size=10000):
        self.connector_param = connector_param
        self.instance_url = 'https://' + connector_param.url_prefix + 'salesforce.com'
        self.token_url = 'https://' + connector_param.url_prefix + 'salesforce.com/services/oauth2/token'
        self.access_token = None
        self.get_token()
        if connector_param.threads:
            self.num_threads = connector_param.threads
        else:
            self.num_threads = 0
        self.bulk = SalesforceBulkExtended(sessionId=self.access_token, host=urlparse(self.instance_url).hostname,
                                           API_version='42.0')
        self.batch_size = batch_size

    def check_token(self):
        try:
            job = self.bulk.create_query_job(object, contentType='CSV')
            test_query = 'SELECT ID FROM Account LIMIT 1'
            batch = self.bulk.query(job, test_query)
            self.connector_wait(job, batch, 'Query done')
            self.bulk.close_job(job)
            return True
        except:
            return False

    def get_token(self):
        if self.access_token == None:
            cached_token = self.get_cached_token()
            if cached_token:
                self.access_token = cached_token
                if not self.check_token():
                    self.get_oauth2_token()
            else:
                self.get_oauth2_token()
        else:
            self.get_oauth2_token()
        return self.access_token

    def get_oauth2_token(self):
        req_param = {
            'grant_type': 'password',
            'client_id': self.connector_param.consumer_key,
            'client_secret': self.connector_param.consumer_secret,
            'username': self.connector_param.username,
            'password': '{password}{security_token}'.format(password=self.connector_param.password,
                                                            security_token=self.connector_param.security_token)
        }
        result = requests.post(self.token_url, headers={"Content-Type": "application/x-www-form-urlencoded"},
                               data=req_param)
        result_dict = loads(result.content)
        if 'access_token' in result_dict.keys():
            self.access_token = result_dict['access_token']
            self.save_token()
            return result_dict['access_token']
        else:
            # print(result_dict)
            return None

    def get_cached_token(self):
        try:
            tokens_dict = load(open(session_file, 'r'))
        except:
            return None
        if self.connector_param.username in tokens_dict.keys():
            return tokens_dict[self.connector_param.username]
        else:
            return None

    def save_token(self):
        tokens_dict = {}
        try:
            tokens_dict = load(open(session_file, 'r'))
        except:
            pass
        tokens_dict[self.connector_param.username] = self.access_token
        dump(tokens_dict, open(session_file, 'w'))

    def remove_token(self):
        tokens_dict = load(open(session_file, 'r'))
        tokens_dict.pop(self.connector_param.username, None)
        dump(tokens_dict, open(session_file, 'w'))

    def _get_soql_template(self, soql):
        stmt = sqlparse.parse(soql)[0]
        soql_template = ''
        find_where = False
        for token in stmt.tokens:
            if str(token).lower().startswith('where'):
                soql_template = soql_template + str(token) + ' AND {0} '
                find_where = True
            else:
                soql_template = soql_template + str(token)
        if not find_where:
            find_from = False
            for token in stmt.tokens:
                if str(token).lower().startswith('from'):
                    find_from = True
                if find_from and (
                        str(token).lower().startswith('order') or str(token).lower().startswith('limit') or str(
                        token).lower().startswith('group')):
                    soql_template = soql_template + ' WHERE {0}'
                soql_template = soql_template + str(token)
        return soql_template

    def fast_load(self, object, soql, header_columns=None, csv_file=None):
        load_threads = 5
        ids_in_batch = 10000
        csv_file_ids = csv_file.split('.')[0] + '_ids.' + csv_file.split('.')[1]
        csv_file_template = csv_file.split('.')[0] + '_from_{0}_to_{1}.' + csv_file.split('.')[1]
        soql_template = self._get_soql_template(soql)
        ids_soql = 'SELECT Id ' + soql[soql.lower().find('from'):]
        # self.bulk_load(object, ids_soql, None, csv_file_ids)
        ids = from_csv_to_dict(csv_file_ids)
        ids_list_rand = []
        for item in ids:
            ids_list_rand.append(item['Id'])
        ids_sorted = sorted(ids_list_rand)
        iteration = 1
        batches = []
        last_id = ids_sorted[0]

        while ids_in_batch * iteration <= len(ids_sorted):
            print(ids_in_batch * iteration)
            batches.append({
                'condition': "(Id>='{0}' AND Id<'{1}')".format(last_id, ids_sorted[ids_in_batch * iteration]),
                'file': csv_file_template.format(last_id, ids_sorted[ids_in_batch * iteration]),
                'soql': soql_template.format(
                    "(Id>='{0}' AND Id<'{1}')".format(last_id, ids_sorted[ids_in_batch * iteration]))
            })
            last_id = ids_sorted[ids_in_batch * iteration]
            iteration = iteration + 1
        batches.append({
            'condition': "(Id>='{0}' AND Id<='{1}')".format(last_id, ids_sorted[-1]),
            'file': csv_file_template.format(last_id, ids_sorted[-1]),
            'soql': soql_template.format("(Id>='{0}' AND Id<='{1}')".format(last_id, ids_sorted[-1]))
        })
        print(len(ids_sorted))
        print(batches)
        self.q_in = Queue(maxsize=load_threads + 1)
        self.q_out = Queue(maxsize=load_threads + 1)
        pool = Pool(load_threads, self.worker_load_batch, (self,))
        for i, batch in enumerate(batches):
            params = MultithreadLoadParam(object, batch['soql'], None, batch['file'], batch['condition'])
            self.q_in.put(params)
            print('put', params)
            if not self.q_out.empty():
                while not self.q_out.empty():
                    batch_result = self.q_out.get()

                    for batch_item in batches:
                        if batch_item['condition'] == batch_result['condition']:
                            batch_item['success'] = batch_result['success']
            sleep(5)
        print('end')
        print(batches)
        self.q_out.close()
        self.q_in.close()
        pool.close()

    def worker_load_batch(self, test):
        while True:
            params = self.q_in.get()
            # batch_executer = SalesforceBulk(sessionId=self.access_token, host=urlparse(self.instance_url).hostname, API_version='37.0')
            batch_executer = RESTConnector(self.connector_param)
            try:
                batch_executer.bulk_load(params.object_name, params.soql, None, params.csv_file)
            except:
                print('error {}'.format(params.condition))
                self.q_out.put({'condition': params.condition, 'success': False})
            else:
                print('fine {}'.format(params.condition))
                self.q_out.put({'condition': params.condition, 'success': True})

    def bulk_load(self, object, soql, header_columns=None, csv_file=None):
        """
        :rtype : executing query and save result in csv or json file
        """
        print(object)
        print('run job {}'.format(soql))
        try:
            job = self.bulk.create_query_job(object)
        except:
            self.access_token = None
            self.get_oauth2_token()
            job = self.bulk.create_query_job(object)
        batch = self.bulk.query(job, soql)
        self.connector_wait(job, [batch], 'Query done')
        self.bulk.close_job(job)
        if csv_file:
            open_mode = 'w'
            with open(csv_file, open_mode) as f_csv:
                writer = csv.writer(f_csv, quoting=csv.QUOTE_ALL)
                for i, result_set in enumerate(
                        self.bulk.get_all_results_for_batch(batch, job_id=job, parse_csv=True, logger=None)):
                    for j, row in enumerate(result_set):
                        if i > 0 and not j:
                            continue
                        writer.writerow(row)
        else:
            dict_adapter = DictAdapter(header_columns, True)
            return dict_adapter.get_dict(self.return_result_rows(
                self.bulk.get_all_results_for_batch(batch, job_id=job, parse_csv=True, logger=None)))

    def upload_batch(self, params):
        batch_csv_io = StringIO.StringIO()
        batch_csv = csv.DictWriter(batch_csv_io, params.header, quoting=csv.QUOTE_ALL)
        batch_csv.writeheader()
        for row in params.batch_data:
            batch_csv.writerow(row)
        return self.bulk.post_bulk_batch(params.job, batch_csv_io.getvalue())

    def worker_upload_batch(self, test):
        while True:
            params = self.q_in.get()
            batch_csv_io = StringIO.StringIO()
            batch_csv = csv.DictWriter(batch_csv_io, params.header, quoting=csv.QUOTE_ALL)
            batch_csv.writeheader()
            for row in params.batch_data:
                batch_csv.writerow(row)
            batch_uploader = SalesforceBulkExtended(sessionId=self.access_token,
                                                    host=urlparse(self.instance_url).hostname, API_version='37.0')
            batch_id = batch_uploader.post_bulk_batch(params.job, batch_csv_io.getvalue())
            print('worker batch number {} batch id {}'.format(params.batch_number, batch_id))
            logging.info('worker batch number {} batch id {}'.format(params.batch_number, batch_id))
            self.q_out.put({'batch_number': params.batch_number, 'batch_id': batch_id})

    def batches_uploader(self, job, data, batch_size=10000, external_keys=None):
        if self.num_threads != 0:
            print('Working with pool. Number of processes {}'.format(self.num_threads))
            logging.info('Working with pool. Number of processes {}'.format(self.num_threads))
            return self.batches_uploader_parllel(job, data, batch_size, external_keys)
        else:
            print('Working in single process')
            logging.info('Working in single process')
            batches = {}
            if type(data) == file:
                dict_csv = csv.DictReader(data.readlines())
            elif type(data) == list:
                dict_csv = data
            batch_data = []
            keys = {}
            batch_size_counter = 0
            # preparing structure for storing results
            if external_keys is not None:
                for ext_key in external_keys:
                    keys[ext_key] = []
            for i, row in enumerate(dict_csv):
                if not i:
                    header = row.keys()
                row_size = len(dumps(row.values()))
                batch_size_counter = batch_size_counter + row_size
                if (not i % batch_size and i > 0) or batch_size_counter > BATCH_SIZE_LIMIT:
                    params = UploaderParam(job, header, batch_data, 0)
                    batches[self.upload_batch(params)] = keys.copy()
                    batch_data = []
                    batch_size_counter = 0
                    # filling key values for storing resiults
                    if external_keys is not None:
                        for ext_key_ in external_keys:
                            keys[ext_key_] = []
                for key in row:
                    if row[key] == 'None':
                        row[key] = None
                batch_data.append(row)
                if external_keys is not None:
                    for ext_key__ in external_keys:
                        keys[ext_key__].append(row[ext_key__])
            if len(batch_data) > 0:
                params = UploaderParam(job, header, batch_data, 0)
                batches[(self.upload_batch(params))] = keys.copy()
            return batches

    def batches_uploader_parllel(self, job, data, batch_size=10000, external_keys=None):
        batches = {}
        if type(data) == file:
            dict_csv = csv.DictReader(data.readlines())
        elif type(data) == list:
            dict_csv = data
        batch_data = []
        keys = {}
        # preparing structure for storing results
        if external_keys is not None:
            for ext_key in external_keys:
                keys[ext_key] = []
        batch_counter = 0
        batches_list = []
        self.q_in = Queue(maxsize=self.num_threads + 1)
        self.q_out = Queue(maxsize=self.num_threads + 1)
        pool = Pool(self.num_threads, self.worker_upload_batch, (self,))
        for i, row in enumerate(dict_csv):
            if not i:
                header = row.keys()
            row_size = len(dumps(row))
            if (not i % batch_size and i > 0) or len(dumps(batch_data)) + row_size > 10000000:
                # TODO need to make it parallel
                # 'job' ,'header', 'batch_data', 'batch_number', 'connection'
                params = UploaderParam(job, header, batch_data, batch_counter)
                self.q_in.put(params)
                # self.upload_batch(job,header,batch_data, batch_counter)
                if not self.q_out.empty():
                    while not self.q_out.empty():
                        batches_list.append(self.q_out.get())
                batches[str(batch_counter)] = keys.copy()
                # batches['batch_number'] = batch_counter
                batch_counter = batch_counter + 1
                batch_data = []
                # filling key values for storing resiults
                if external_keys is not None:
                    for ext_key_ in external_keys:
                        keys[ext_key_] = []
            for key in row:
                if row[key] == 'None':
                    row[key] = None
            batch_data.append(row)
            if external_keys is not None:
                for ext_key__ in external_keys:
                    keys[ext_key__].append(row[ext_key__])

        if len(batches_list) != batch_counter:
            while not len(batches_list) == batch_counter:
                if not self.q_out.empty():
                    batches_list.append(self.q_out.get())
        self.q_out.close()
        self.q_in.close()
        # cahnge numbered key name to corresponding batch id
        for batch_item in batches:
            for batch_list_item in batches_list:
                if str(batch_list_item['batch_number']) == batch_item:
                    batches[batch_list_item['batch_id']] = batches.pop(batch_item)

        if len(batch_data) > 0:
            params = UploaderParam(job, header, batch_data, 0)
            batches[(self.upload_batch(params))] = keys.copy()
        # for batch_id in batches:
        #     print(batch_id)
        pool.close()
        return batches

    def bulk_insert(self, object, data, external_keys=None, batch_size=BULK_BATCH_SIZE,
                    concurrency=BULK_CONCURRENCY_MODE):
        job = self.bulk.create_insert_job(object, contentType='CSV', concurrency=concurrency)
        batches = self.batches_uploader(job, data, batch_size=batch_size, external_keys=external_keys)
        self.connector_wait(job, batches, 'bulk insert done')
        self.bulk.close_job(job)
        return self.return_result_batches(job, batches, external_keys=external_keys)

    def bulk_update(self, object, data, external_keys=None, batch_size=BULK_BATCH_SIZE,
                    concurrency=BULK_CONCURRENCY_MODE):
        job = self.bulk.create_update_job(object, contentType='CSV', concurrency=concurrency)
        batches = self.batches_uploader(job, data, batch_size=batch_size, external_keys=external_keys)
        self.connector_wait(job, batches, 'bulk update done')
        self.bulk.close_job(job)
        return self.return_result_batches(job, batches, external_keys=external_keys)

    def bulk_delete(self, object, ids=[], where=None, external_keys=None, batch_size=BULK_BATCH_SIZE,
                    concurrency=BULK_CONCURRENCY_MODE):
        if len(ids) == 0 and where == None:
            return []
        if len(ids) == 0:
            soql = "SELECT Id FROM {object} WHERE {where}".format(object=object, where=where)
            records_for_deleting = self.bulk_load(object, soql)
            if len(records_for_deleting) == 0:
                print('Nothing to delete')
                return None
            json.dump(records_for_deleting, open('data/event_deletion.json', 'w'))
            delete_job = self.bulk.create_job(object_name=object, operation='delete')
            batches = self.batches_uploader(job=delete_job, data=records_for_deleting, batch_size=self.batch_size,
                                            external_keys=external_keys)
        self.connector_wait(job=delete_job, batches_in=batches, ending_message='deletion done')
        self.bulk.close_job(delete_job)
        return self.return_result_batches(delete_job, batches, external_keys=external_keys)

    def bulk_simple_delete(self, object_name, records_for_deleting, batch_size=BULK_BATCH_SIZE,
                           concurrency=BULK_CONCURRENCY_MODE):
        external_keys = ['Id']
        delete_job = self.bulk.create_job(object_name=object_name, operation='delete', concurrency=concurrency)
        batches = self.batches_uploader(job=delete_job, data=records_for_deleting, batch_size=batch_size,
                                        external_keys=external_keys)
        self.connector_wait(job=delete_job, batches_in=batches, ending_message='deletion done')
        self.bulk.close_job(delete_job)
        return self.return_result_batches(delete_job, batches, external_keys=external_keys)

    def bulk_upsert(self, object, external_id_name, data, batch_size=BULK_BATCH_SIZE,
                    concurrency=BULK_CONCURRENCY_MODE):
        job = self.bulk.create_upsert_job(object_name=object, external_id_name=external_id_name, contentType='CSV',
                                          concurrency=concurrency)
        batches = self.batches_uploader(job=job, data=data, batch_size=batch_size)
        self.connector_wait(job, batches, 'upserting done')
        self.bulk.close_job(job)
        return self.return_result_batches(job, batches, external_keys=external_id_name)

    def return_result_rows(self, all_results):
        rows = []
        for result_set in all_results:
            for row in result_set:
                rows.append(row)
        return rows

    def return_result_batches(self, job, batches, external_keys=None):
        res = []
        for batch in batches:
            if self.get_batch_result_iter(job, batch, True, None):
                for i, row in enumerate(self.get_batch_result_iter(job, batch, True, None)):
                    if external_keys is not None:
                        for key in batches[batch]:
                            row[key] = batches[batch][key][i]
                    res.append(row)
        return res

    def connector_wait(self, job, batches_in, ending_message=''):
        batches = []
        for batch in batches_in:
            batches.append(batch)
        if len(batches) == 0:
            print('Nonthing to upload')
            return None
        wait_message = 'Wait for job done. batches finished {0}, failed {1}, total batches {2}'
        clock = 0
        total_batches = len(batches)
        closed_count = 0
        failed_count = 0
        while True:
            if clock == 10:
                clock = 0
                for batch in batches:
                    try:
                        if self.bulk.is_batch_done(job, batch):
                            closed_count = closed_count + 1
                            batches.remove(batch)
                    except:
                        failed_count = failed_count + 1
                        batches.remove(batch)
            sleep(0.5)
            clock = clock + 1
            spin(wait_message.format(closed_count, failed_count, total_batches))
            if closed_count + failed_count == total_batches or len(batches) == 0:
                break
        print('\r' + ending_message.ljust(
            len(ending_message) if len(ending_message) > len(wait_message) + 4 else len(wait_message) + 4))

    def get_batch_result_iter(self, job_id, batch_id, parse_csv=False,
                              logger=None):
        """

        **** This code snippet was taken from salesforce bulk library ****

        Return a line interator over the contents of a batch result document. If
        csv=True then parses the first line as the csv header and the iterator
        returns dicts.
        """
        status = self.bulk.batch_status(job_id, batch_id)
        if status['state'] != 'Completed':
            return None
        elif logger:
            if 'numberRecordsProcessed' in status:
                logger("Bulk batch %d processed %s records" %
                       (batch_id, status['numberRecordsProcessed']))
            if 'numberRecordsFailed' in status:
                failed = int(status['numberRecordsFailed'])
                if failed > 0:
                    logger("Bulk batch %d had %d failed records" %
                           (batch_id, failed))
        uri = self.bulk.endpoint + \
              "/job/%s/batch/%s/result" % (job_id, batch_id)
        r = requests.get(uri, headers=self.bulk.headers(), stream=True)

        # print(type(r))
        # print(r.text)
        # print(r.keys())
        # result_id = r.text.split("<result>")[1].split("</result>")[0]

        # uri = self.bulk.endpoint + \
        #     "/job/%s/batch/%s/result/%s" % (job_id, batch_id, result_id)
        # r = requests.get(uri, headers=self.bulk.headers(), stream=True)

        if parse_csv:
            return csv.DictReader(r.iter_lines(chunk_size=2048), delimiter=",",
                                  quotechar='"')
        else:
            return r.iter_lines(chunk_size=2048)
