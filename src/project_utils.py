import logging
import cStringIO

__author__ = 'Volodymyr Varchuk'
__email__ = "vladimir.varchuk@rackspace.com"


import sys

from json import dump, load
import datetime
import os
import csv
import tempfile
import time
from collections import namedtuple
import gzip
from os import path as p
from configparser import ConfigParser
from cStringIO import StringIO


arch_extension = 'gz'
chunk_size = 10*1024*1024


BackupObject = namedtuple('BackupObject',
                            ['object', 'source', 'sql'])

new_accounts_list_condition = ''


def copy_file(in_file, out_file, mode):
    with open(in_file) as f_in:
        with open(out_file, mode) as f_out:
            for line in f_in:
                f_out.write(line)
            f_out.write('\n')



def get_default_user_id():
    config_file = 'config.ini'
    config = ConfigParser()
    with open(config_file, 'r') as conf_file:
        config.read_file(conf_file)
    return config['default_user_id']['DEFAULT_USER_ID']


# Print iterations progress
def printProgress (iteration, total, prefix = '', suffix = '', decimals = 1, barLength = 100):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        barLength   - Optional  : character length of bar (Int)
    """
    formatStr       = "{0:." + str(decimals) + "f}"

    if total == 0:
        percents        = formatStr.format(100)
        return 0

    percents        = formatStr.format(100 * (iteration / float(total)))
    filledLength    = int(round(barLength * iteration / float(total)))
    bar             = '=' * filledLength + '-' * (barLength - filledLength)
    sys.stdout.write('\r%s |%s| %s%s %s' % (prefix, bar, percents, '%', suffix)),
    sys.stdout.flush()
    if iteration >= total:
        percents        = formatStr.format(100 * (total / float(total)))
        sys.stdout.write('\r%s |%s| %s%s %s\n' % (prefix, bar, percents, '%', suffix)),
        sys.stdout.flush()
        return 0
    return 1


def success_records_check( data, key_name='Success', save_errors=None):
    success_count = 0
    if not data:
        return None
    errors = []
    for element in data:
        # print(element)
        # print(key_name)
        if type(element) is list:
            el_t = element[0]
        else:
            el_t = element
        if el_t[key_name] == True or str(el_t[key_name]).lower() == 'True'.lower():
            success_count = success_count + 1
        else:
            if save_errors is not None:
                errors.append(el_t)
    if save_errors is not None:
        save_errors_csv = '.'.join(save_errors.split('.')[:-1] + ['csv'])
        conv_err = []
        for error_rec in errors:
            err_rec_conv = {}
            for err_key in error_rec:
                err_rec_conv[err_key] = unicode(error_rec[err_key]).encode("utf-8")
            conv_err.append(err_rec_conv)
        if len(conv_err) != 0:
            keys = conv_err[0].keys()
        else:
            keys = data[0].keys()
        with open(save_errors_csv, 'wb') as output_file:
            dict_writer = csv.DictWriter(output_file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(conv_err)
        dump(errors, open(save_errors,'w'))
    return '{0}/{1}'.format(success_count, len(data))

class Timer:
    def __init__(self):
        self.start_time = datetime.datetime.now()
        self.stop_time = None


    def start(self):
        self.start_time = datetime.datetime.now()


    def stop(self):
        self.stop_time = datetime.datetime.now()
        return self.stop_time - self.start_time


def check_result(message, result_file_name):
    if os.path.isfile(result_file_name):
        save_erros = p.join( p.split(result_file_name)[0], p.split(result_file_name)[1].split('.')[0] + 'errors.json')
        # save_erros = '/'.join(result_file_name.split('/')[:-1] + [result_file_name.split('/')[-1]])
        res_data = load(open(result_file_name))
        res_message = success_records_check(res_data, key_name='Success', save_errors=save_erros)
        if res_message:
            res_message_rtext = message + res_message
        else:
            res_message_rtext = message + ' batch was empty'
        print (res_message_rtext)
        logging.info(res_message_rtext)
        return save_erros

def spin(text):
    spin.symbol = (1 + spin.symbol) % 7

    symbols = ['|','/','-','\\','|','/','-']
    sys.stdout.write( '\r%s %s' % (text, symbols[spin.symbol])),
    sys.stdout.flush()
spin.symbol = 0


def get_object_name(soql):
    pos = soql.lower().index(' from '.lower())
    object = None
    for word in soql[pos+6:].split(' '):
        if word != '':
            object = word
            break
    return object


def check_and_create_dir(dir_name):
    if not os.path.isdir(dir_name):
        logging.info("created directory {}".format(dir_name))
        os.makedirs(dir_name)

def check_working_dir(objects_list):
    directory_list = ['data', 'data/local_cache']
    for object_item in objects_list:
        directory_list.append(os.path.join(directory_list[0], object_item))
    for dir_item in directory_list:
        check_and_create_dir(dir_item)


bkp_fmt='data/backup/{object_name}_{source}_{date_time}.csv'
SRC = 'src'
DST = 'dst'

def get_backup_name(object_name, source):
    return bkp_fmt.format(object_name=object_name, source=source, date_time=time.strftime("%Y%m%d_%H%M%S"))


def get_backup_cmd (backup_object):
    cmd_item =  {
        'execute':{
            'input_data':backup_object.sql,
            'connector':backup_object.source,
            'object': ''.join([ i for i in backup_object.object if not i.isdigit()]),
            'command':'query',
            'tag':None,
            'output_data':get_backup_name(backup_object.object, backup_object.source),
            'message':'Getting data from {source} {object}'.format(source=backup_object.source, object=backup_object.object)
        }
    }
    return cmd_item


def get_backup_cmd_objects(backup_objects):
    cmd_sequence = []
    for item in backup_objects:
        cmd_sequence.append(get_backup_cmd(item))
    return cmd_sequence


def from_csv_to_dict(csv_file):
    csv_dict = []
    if not os.path.exists(csv_file):
        return []
    with open(csv_file) as f:
        csv_dict = [{k: v for k, v in row.items()}
                             for row in csv.DictReader(f, skipinitialspace=True)]
    return csv_dict


def to_csv_from_dict(data, file_name):
    if len(data) != 0:
        keys = data[0].keys()
    with open(file_name, 'wb') as output_file:
            dict_writer = csv.DictWriter(output_file, keys)
            dict_writer.writeheader()
            dict_writer.writerows(data)

def zip_files(files):
    for file_in in files:
        file_gz = '.'.join([file_in, arch_extension])
        if os.path.isfile(file_gz):
            continue
        try:
            with open(file_in, 'r') as f_in, gzip.open(file_gz, 'ab') as f_out:
                while True:
                    data = f_in.read(chunk_size)
                    if not data:
                        break
                    f_out.write(data)
            f_out.close()
            print('Compressing {} ---> {} [DONE]'.format(file_in, file_gz))
        except IOError:
            print('Compressing {} ---> {} [ERROR]'.format(file_in, file_gz))


def account_ids_list(in_file, field_name='Id'):
    csv_dict = from_csv_to_dict(in_file)
    ids = []
    for item in csv_dict:
        ids.append(item[field_name])
    return ids

def account_ids_str_condition(ids_list):
    ids_str = ", ".join(["'{0}'".format(id_val) for id_val in ids_list])
    return ids_str

def path_handler(file_path):
    if 'MRIYA_PATH' in os.environ.keys():
        working_path = os.environ['MRIYA_PATH']
    else:
        working_path = None
    if file_path[0] != '/' and working_path is not None:
        return os.path.join(working_path, file_path)
    else:
        return None

class Capturing(list):
    def __enter__(self):
        self._stdout = sys.stdout
        sys.stdout = self._stringio = StringIO()
        return self

    def __exit__(self, *args):
        self.extend(self._stringio.getvalue().splitlines())
        del self._stringio    # free up some memory
        sys.stdout = self._stdout

class RecordCountBackground():
    pass
