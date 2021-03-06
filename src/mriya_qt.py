import StringIO
from os.path import dirname

__author__ = 'Volodymyr Varchuk'
__email__ = 'vladimir.varchuk@rackspace.com'

'''
Mriya Query Tool
================

Application for extracting and querying SalesForce data using SQL (SQLite engine)

'''

__version__ = '1.0'

from kivy.config import Config

Config.set('input', 'mouse', 'mouse,multitouch_on_demand')
# Config.set('kivy','icon','/home/volodymyr/git/dev_MriyaQt/icons/mriyaQT.ico')

import json
import re
import sys
from os import path as p, listdir
from os import mkdir

from kivy.app import App, Builder
from kivy.uix.screenmanager import ScreenManager, Screen, SlideTransition
from kivy.properties import ListProperty, StringProperty, \
    NumericProperty, BooleanProperty
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.dropdown import DropDown
from kivy.uix.textinput import TextInput
from kivy.uix.popup import Popup
from kivy.uix.settings import SettingsWithSidebar
from data_connector import RESTConnector, MsSQLConnector
from configparser import ConfigParser
from data_connector import get_conn_param
from migration_engine import MigrationWorkflow
from sql_view import SQLView
from sf_view import TaskView
from start_screen import StartScreen
from review_screen import ReviewScreen
from sf_execute_view import BatchExecuteView
from project_utils import Capturing, Timer, copy_file
from sys import argv
from datetime import datetime
from mssql_view import MSSQL_Query
import csv

default_project_dir = p.join('/home/volodymyr/work', 'test_exec', 'test_exec.mpr')
default_application_dir = p.join(p.expanduser('~'), 'work')

standart_object_list_file = p.join('StandartObjectList.ini')

StandartObjectList = ['Account', 'Contact', 'AccountContactRole', 'Lead', 'Opportunity', 'Task', 'Event', 'Note',
                      'Attachment',
                      'Campaign', 'CampaignMember', 'User', 'AccountTeamMember']

if p.exists(standart_object_list_file):
    jj = json.load(open(standart_object_list_file))
    StandartObjectList = jj[0]['StandartObjectList']

StandartObjectList_uppercase = [object_item.upper() for object_item in StandartObjectList]

ObjectsList = []

config_file = argv[1] if len(argv) >= 2 else p.join('config.ini')
sf_object = None
sf_source = None

config = ConfigParser()
with open(config_file, 'r') as conf_file:
    config.read_file(conf_file)
# print(config.keys()[1:])
# print([config[key].keys() for key in config.keys()[1:]])
# exit(0)

if 'type' in config[config.keys()[1]].keys():
    SourceList = [{'type': config[key]['type'], 'name': key} for key in config.keys()[1:]]
else:
    SourceList = [{'type': 'sf', 'name': key} for key in config.keys()[1:]]

print(SourceList)
ObjectsMetadata = []


def get_fields_rest(connection, sobject):
    rc = RESTConnector(get_conn_param(config[connection]))
    req_res = json.loads('[{}]'.format(rc.bulk.rest_request('sobjects/{}/describe'.format(sobject))))
    fields = []
    for field_item in req_res[0]['fields']:
        fields.append(field_item['name'])
    return sorted(fields)


kv_dir = p.join(dirname(dirname(__file__)), 'kv')
main_kv_file = p.join(kv_dir, 'MriyaQT.kv')


def load_kvs():
    first_kv_file = p.join(kv_dir, 'mriya_qt.kv')
    with open(main_kv_file, "w") as f:
        copy_file(first_kv_file, main_kv_file, 'w')
        for kv in listdir(kv_dir):
            kv_file = p.join(kv_dir, kv)
            if kv_file not in [first_kv_file, main_kv_file]:
                copy_file(kv_file, main_kv_file, 'a')


load_kvs()
Builder.load_file(main_kv_file)


class Project():
    def __init__(self, file_name=default_project_dir):
        if not p.exists(file_name):
            mkdir(file_name)
            mkdir(p.join(file_name, 'data'))
            self.project_file_name = p.join(p.join(p.dirname(file_name), p.basename(file_name)),
                                            p.basename(file_name) + '.mpr')
            self.project_dir = file_name
            self.project_data_dir = p.join(self.project_dir, 'data')
            self.project_name = p.basename(file_name)
            self.project = {
                'metadata': {
                    source_name: {} for source_name in config.keys()},
                'workflow': [],
                'project_name': self.project_name,
                'project_data_dir': self.project_data_dir,
                'project_dir': self.project_dir
            }
        else:
            self.project_file_name = file_name
            self.project = json.load(open(file_name))
            self.project_timestamp = p.getmtime(self.project_file_name)
            self.project_dir = self.project['project_dir']
            self.project_data_dir = self.project['project_data_dir']
            self.project_name = self.project['project_name']
            for source_item in self.get_sources('sf'):
                if source_item not in self.project['metadata']:
                    self.project['metadata'][source_item] = {}

    def save(self):
        json.dump(self.project, open(self.project_file_name, 'w'))
        self.project_timestamp = p.getmtime(self.project_file_name)
        if p.exists(self.project_file_name):
            recent_projetcs_file = 'recent_projects.ini'
            projects = open(recent_projetcs_file).read().splitlines()
            if self.project_file_name in projects:
                projects.remove(self.project_file_name)
            projects = [self.project_file_name] + projects
            with open(recent_projetcs_file, 'w') as f:
                f.writelines('\n'.join(projects))

    def get_sobjects(self, connection, force_refresh=False):
        if connection not in self.get_sources('sf'):
            return []
        if self.project['metadata'][connection] == {} or force_refresh:
            rest_connector = RESTConnector(get_conn_param(config[connection]))
            req_res = json.loads('[{}]'.format(rest_connector.bulk.rest_request('sobjects')))[0]['sobjects']
            self.project['metadata'][connection] = {
            (type(sobject['name']) is str or type(sobject['name']) is unicode) and sobject['name']: {} for sobject in
            req_res}
        return sorted(self.project['metadata'][connection].keys())

    def get_sobject_fileds(self, connection, sobject, force_refresh=False):
        if self.project['metadata'][connection] == {}:
            self.get_sobjects(connection)
        if force_refresh or self.project['metadata'][connection][sobject] == {}:
            rest_connector = RESTConnector(get_conn_param(config[connection]))
            req_res = json.loads(
                '[{}]'.format(rest_connector.bulk.rest_request('sobjects/{}/describe'.format(sobject))))
            fields = []
            for field_item in req_res[0]['fields']:
                fields.append(field_item['name'])
            self.project['metadata'][connection][sobject] = sorted(fields)
        return sorted(self.project['metadata'][connection][sobject])

    def get_standart_sobjects(self, connection, force_refresh=False):
        objects_list = self.get_sobjects(connection, force_refresh=force_refresh)
        show_object_list = []

        for object_item in objects_list:
            if object_item.upper() in StandartObjectList_uppercase:
                show_object_list.append(object_item)
        return show_object_list

    def get_sources(self, source_type='sf'):
        Sources_list = []
        for sourcelist_item in SourceList:
            if type(sourcelist_item) == dict:
                if str(sourcelist_item['type']) == source_type:
                    Sources_list.append(sourcelist_item['name'])
            else:
                Sources_list.append(sourcelist_item)
        return Sources_list

    def get_object_from_sql(self, soql):
        res = None
        soql_upper = soql
        try:
            res = re.search(' FROM (.*?) WHERE ', soql_upper, re.IGNORECASE).group(1).strip()
        except:
            try:
                res = re.search(' FROM (.*?) GROUP ', soql_upper, re.IGNORECASE).group(1).strip()
            except:
                try:
                    res = re.search(' FROM (.*?) ORDER ', soql_upper,
                                    re.IGNORECASE).group(1).strip()
                except:
                    try:
                        res = re.search(' FROM (.*?) LIMIT ', soql_upper,
                                        re.IGNORECASE).group(1).strip()
                    except:
                        res = soql[soql_upper.find(' FROM ') + 6:].strip()
        return res

    def get_sql_after_from(self, soql):
        soql_upper = soql.upper()
        after_from = soql[soql_upper.find(' FROM '):].strip().split(' ')
        res = ' '.join(after_from[2:])
        return res

    def get_fields_from_sql(self, sql):
        re_res = re.search('SELECT(.*?)FROM', sql, re.IGNORECASE).group(1)
        sql_fields = None
        if re_res:
            sql_fields = [field_item.strip() for field_item in re_res.split(',')]
        return sql_fields

    def _get_fields_from_csv(self, csv_file):
        field_names = []
        if p.exists(csv_file):
            with open(csv_file) as f:
                field_names = f.readline().replace('"', '').strip().split(',')
        return field_names


class ComboEdit(TextInput):
    options = ListProperty()

    def __init__(self, **kw):
        ddn = self.drop_down = DropDown()
        ddn.bind(on_select=self.on_select)
        super(ComboEdit, self).__init__(**kw)

    def on_options(self, instance, value):
        ddn = self.drop_down
        ddn.clear_widgets()
        for widg in value:
            widg.bind(on_release=lambda btn: ddn.select(btn.text))
            ddn.add_widget(widg)

    def on_select(self, *args):
        self.text = args[1]

    def on_touch_up(self, touch):
        if touch.grab_current == self:
            self.drop_down.open(self)
        return super(ComboEdit, self).on_touch_up(touch)


class TaskListItem(BoxLayout):
    task_content = StringProperty()
    task_title = StringProperty()
    task_index = NumericProperty()
    task_command = StringProperty()
    task_type = StringProperty()
    task_exec = BooleanProperty()
    task_status = StringProperty()

    def __init__(self, **kwargs):
        del kwargs['index']
        super(TaskListItem, self).__init__(**kwargs)

    def refresh_status(self):
        if self.task_status == 'working':
            print(self.task_status)

    def get_text_color(self):

        color = (0.5, 0.5, 0.7, 1)
        if self.task_type == 'SF_Execute':
            if self.task_command == 'delete':
                color = (0.7, 0.5, 0.6, 1)
            elif self.task_command == 'update':
                color = (0.6, 0.7, 0.7, 1)
            else:
                color = (0.6, 0.6, 0.7, 1)
        if self.task_type == 'SF_Query':
            color = (0.6, 0.6, 0.6, 1)
        if self.task_type == 'MSSQL_Query':
            color = (0.7, 0.7, 0.6, 1)
        return color

    def get_color(self):
        color = (0.3, 0.3, 0.5, 1)
        if self.task_type == 'SF_Execute':
            if self.task_command == 'delete':
                color = (0.5, 0.3, 0.4, 1)
            elif self.task_command == 'update':
                color = (0.4, 0.5, 0.5, 1)
            else:
                color = (0.4, 0.4, 0.5, 1)
        if self.task_type == 'SF_Query':
            color = (0.4, 0.4, 0.4, 1)
        if self.task_type == 'MSSQL_Query':
            color = (0.5, 0.5, 0.45, 1)
        return color

    def get_icon(self):
        prefix = p.join('icons', 'flat_color_icons')
        if self.task_type == 'SF_Execute':
            return p.join(prefix, 'upload-48.png')
        if self.task_type == 'SF_Query':
            return p.join(prefix, 'icons8-salesforce-48.png')

        if self.task_type == 'MSSQL_Query':
            return p.join(prefix, 'icons8-sql-48.png')
        return p.join(prefix, 'icons8-csv-48.png')


class Tasks(Screen):
    data = ListProperty()

    def args_converter(self, row_index, item):
        # if item is SF_Execute type it may have command key
        if 'command' in item.keys():
            if 'task_external_id_name' in item.keys():
                return {
                    'task_index': row_index,
                    'task_title': item['title'],
                    'task_type': item['type'],
                    'task_command': item['command'],
                    'task_exec': item['exec'],
                    'task_status': item['status']
                }
            else:
                return {
                    'task_index': row_index,
                    'task_title': item['title'],
                    'task_type': item['type'],
                    'task_command': item['command'],
                    'task_exec': item['exec'],
                    'task_status': item['status']
                }
        else:
            return {
                'task_index': row_index,
                'task_title': item['title'],
                'task_type': item['type'],
                'task_exec': item['exec'],
                'task_status': item['status']
            }


class TaskApp(App):
    def __init__(self, **kwargs):
        super(TaskApp, self).__init__(**kwargs)
        self.default_project_dir = default_project_dir
        self.default_application_dir = default_application_dir

    def build(self):
        self.icon = 'icons/mriyaQT.ico'
        self.settings_cls = SettingsWithSidebar
        self.start_screen = StartScreen()
        self.transition = SlideTransition(duration=.35)
        root = ScreenManager(transition=self.transition)
        root.add_widget(self.start_screen)
        self.title = '[{0}]'.format(config_file)
        return root

    def load_tasks(self):
        data = self.project.project['workflow']
        self.tasks.data = data

    def save_tasks(self):
        if p.exists(self.project.project_file_name):
            if self.project.project_timestamp <> p.getmtime(self.project.project_file_name):
                content_str = '''
BoxLayout:
    orientation:'vertical'
    Label:
        halign:'center'
        text: "Project file was changed by another application.\\nDo you want to apply your current changes anyway?"
    BoxLayout:
        orientation:'horizontal'
        Button:
            text: "Yes"
            on_release: app.do_save()
        Button:
            text: "No"
            on_release: app.popup.dismiss()
            '''.format()
                self.popup = Popup(title='Delete task',
                                   content=Builder.load_string(content_str),
                                   size_hint=(None, None), size=(400, 150),
                                   auto_dismiss=False)
                self.popup.open()
            else:
                self.do_save()
        else:
            self.do_save()

    def do_save(self):
        if hasattr(self, 'popup'):
            self.popup.dismiss()
        self.project.project['workflow'] = self.tasks.data
        self.project.save()

    def call_del_task(self, task_index):
        content_str = '''
BoxLayout:
    orientation:'vertical'
    Label:
        text: "Are you sure you want to delete {task_name}?"
    BoxLayout:
        orientation:'horizontal'
        Button:
            text: "Yes"
            on_release: app.detete_task (int({task_index}), 'Yes')
        Button:
            text: "No"
            on_release: app.detete_task (int({task_index}), 'No')
        '''.format(task_name=self.tasks.data[task_index]['title'], task_index=task_index)
        self.popup = Popup(title='Delete task',
                           content=Builder.load_string(content_str),
                           size_hint=(None, None), size=(400, 150),
                           auto_dismiss=False)
        self.popup.open()

    def detete_task(self, task_index, answer):
        self.popup.dismiss()
        if answer == 'Yes':
            del self.tasks.data[task_index]
            self.save_tasks()
            self.refresh_tasks()
            self.go_tasks(task_index)
        else:
            print('task {0} was not removed'.format(task_index))

    def edit_task(self, task_index):
        task = self.tasks.data[task_index]
        name = 'task{}'.format(task_index)

        if self.root.has_screen(name):
            self.root.remove_widget(self.root.get_screen(name))

        if task.get('type') == 'SF_Query':
            view = TaskView(
                name=name,
                task_index=task_index,
                task_title=task.get('title'),
                task_content=task.get('content'),
                task_sql=task.get('sql'),
                task_type=task.get('type'),
                task_input=task.get('input'),
                task_output=task.get('output'),
                task_source=task.get('source'),
                task_exec=task.get('exec'),
                project=self.project,
                sources_list=self.project.get_sources('sf')
            )
            if task.get('source') != '':
                view.objects_list = self.project.get_standart_sobjects(task.get('source'))
        elif task.get('type') == 'SQL_Query':
            view = SQLView(
                name=name,
                task_index=task_index,
                task_title=task.get('title'),
                task_content=task.get('content'),
                task_sql=task.get('sql'),
                task_type=task.get('type'),
                task_input=task.get('input'),
                task_output=task.get('output'),
                task_source=task.get('source'),
                task_exec=task.get('exec'),
                project=self.project
            )
            view.task_list.adapter.data = [task_name['title'] for task_name in self.tasks.data[:task_index]]
            view.task_ouputs_dict = {task_name['title']: task_name['output'] for task_name in
                                     self.tasks.data[:task_index]}
        elif task.get('type') == 'SF_Execute':
            print('execute type')
            view = BatchExecuteView(
                name=name,
                task_index=task_index,
                task_title=task.get('title'),
                task_content=task.get('content'),
                task_sql=task.get('sql'),
                task_command=task.get('command'),
                task_type=task.get('type'),
                task_input=task.get('input'),
                task_output=task.get('output'),
                task_source=task.get('source'),
                task_exec=task.get('exec'),
                task_batch_size=task.get('batch_size') if task.get('batch_size') else '',
                task_concurrency=task.get('concurrency') if task.get('concurrency') else '',
                task_api_type=task.get('api_type'),
                task_external_id_name=task.get('external_id_name'),
                sources_list=self.project.get_sources('sf'),
                preview_text='',
                project=self.project
            )
            view.task_list = [task_name['title'] for task_name in self.tasks.data[:task_index]]
            view.task_ouputs_dict = {task_name['title']: task_name['output'] for task_name in
                                     self.tasks.data[:task_index]}
        elif task.get('type') == 'MSSQL_Query':
            view = MSSQL_Query(
                name=name,
                task_index=task_index,
                task_title=task.get('title'),
                task_sql=task.get('sql'),
                task_output=task.get('output'),
                task_source=task.get('source'),
                sources_list=self.project.get_sources('mssql'),
                project=self.project

            )
        self.root.add_widget(view)
        self.transition.direction = 'left'
        self.root.current = view.name

    def add_task(self, task_type):
        task_names_index = []
        projectname = self.project.project_name
        for task_name_item in [task_item['title'] for task_item in self.tasks.data]:
            if task_name_item.startswith(projectname) and task_name_item[len(projectname):].isdigit():
                task_names_index.append(task_name_item)
        max_index = 1
        if task_names_index:
            max_index = int(max(task_names_index)[len(projectname):]) + 1
        new_title_name = '{0}{1:02d}'.format(projectname, max_index)
        self.tasks.data.append({'title': new_title_name, 'content': '', 'sql': '', 'type': task_type, 'input': ' ',
                                'output': p.join(self.project.project_data_dir, new_title_name + '.csv'), 'source': '',
                                'exec': False, 'status': 'idle', 'external_id_name': 'Id', 'command': 'update'})
        task_index = len(self.tasks.data) - 1
        self.edit_task(task_index)

    def refresh_tasks(self):
        data = self.tasks.data
        self.tasks.data = []
        self.tasks.data = data
        self.tasks.ids.lv_tasks._reset_spopulate()

    def go_tasks(self, task_item):
        self.save_tasks()
        self.refresh_tasks()
        self.transition.direction = 'right'
        self.root.current = 'tasks'

    def up_task(self, task_index):
        if task_index != 0:
            self.tasks.data[task_index], self.tasks.data[task_index - 1] = self.tasks.data[task_index - 1], \
                                                                           self.tasks.data[task_index]
        self.save_tasks()
        self.refresh_tasks()

    def down_task(self, task_index):
        if task_index != len(self.tasks.data) - 1:
            self.tasks.data[task_index], self.tasks.data[task_index + 1] = self.tasks.data[task_index + 1], \
                                                                           self.tasks.data[task_index]
        self.save_tasks()
        self.refresh_tasks()

    def get_task_names(self):
        return [task_item['title'] for task_item in self.tasks.data]

    def save_task(self, task_index, data):
        existing_names = [title_name['title'] for i, title_name in enumerate(self.tasks.data) if i <> task_index]
        if data['title'] in existing_names:
            print ('name already exists')
            content_str = '''
BoxLayout:
    orientation:'vertical'
    Label:
        halign:'center'
        text: "Poject was not saved. \\nTask with name {task_name} already exists. \\nPlease specify another name."
    BoxLayout:
        orientation:'horizontal'
'''.format(task_name=self.tasks.data[task_index]['title'], task_index=task_index)
            self.popup = Popup(title='Wrong task name',
                               content=Builder.load_string(content_str),
                               size_hint=(None, None), size=(400, 150),
                               auto_dismiss=True)
            self.popup.open()
        else:
            for data_item in data:
                self.tasks.data[task_index][data_item] = data[data_item]
            self.save_tasks()

    def on_skip_task(self, instance, value, task_index):
        self.tasks.data[task_index]['exec'] = value

    def go_to_project(self, project_file):
        if p.exists(project_file):
            recent_projetcs_file = 'recent_projects.ini'
            projects = open(recent_projetcs_file).read().splitlines()
            if project_file in projects:
                projects.remove(project_file)
            projects = [project_file] + projects
            with open(recent_projetcs_file, 'w') as f:
                f.writelines('\n'.join(projects))
        self.project = Project(project_file)
        self.tasks = Tasks(name='tasks')
        self.load_tasks()
        self.root.add_widget(self.tasks)
        self.transition.direction = 'left'
        self.root.current = self.tasks.name
        self.title = 'MriyaQT - {0} :: [{1}]'.format(p.basename(project_file).split('.')[0], config_file)

    def go_to_review_screen(self, output_path, errors_only=False):
        o_path = output_path
        if errors_only:
            o_path = '.'.join(
                output_path.split('.')[:-2] + [output_path.split('.')[-2] + 'errors'] + [output_path.split('.')[-1]])
        review_screen = ReviewScreen(name=p.basename(o_path), file_name=o_path, previous_screen=self.root.current)
        if self.root.has_screen(review_screen.name):
            self.root.remove_widget(self.root.get_screen(review_screen.name))
        self.root.add_widget(review_screen)
        self.transition.direction = 'left'
        self.root.current = review_screen.name

    def go_back_to_task(self, previous_screen):
        self.transition.direction = 'right'
        self.root.current = previous_screen

    def goto_settings(self):
        self.open_settings()
        pass

    def build_config(self, config):
        config.setdefaults('mriya', {
            'batch_size': 5000,
            'concurrency': 'Parallel',
            'api_type': 'BulkAPI',
            'ctrl_key': 305,
            'alt_key': 308})

    def build_settings(self, settings):
        settings.add_json_panel('Mriya settings',
                                self.config,
                                data=open('settings_template.json', 'r').read())

    def get_record_count(self, task_index):
        print(task_index)
        tt = ''

        if p.exists(self.tasks.data[task_index]['output']):
            tt = datetime.utcfromtimestamp(p.getmtime(self.tasks.data[task_index]['output'])).strftime(
                '%Y-%m-%d %H:%M:%S.%M')
            print(tt)
            csv_file = csv.reader(open(self.tasks.data[task_index]['output']))
            row_count = sum(1 for row in csv_file)
            return "{0} [{1}]".format(str(tt), row_count)
        return ''

    def cal_exec_workflow(self):
        delete_operation_count = 0
        for task_item in self.tasks.data:
            if task_item['exec'] and task_item['type'] == 'SF_Execute' and task_item['command'] == 'delete':
                delete_operation_count = delete_operation_count + 1
        if delete_operation_count > 0:
            content_str = '''
BoxLayout:
    orientation:'vertical'
    Label:
        halign:'center'
        height:100
        text: "Are you sure that you want to execute \\n'DELETE' \\noperation in SalesForce for {del_count} task{end}?"
    BoxLayout:
        orientation:'horizontal'
        Button:
            text: "Yes"
            on_release: app.exec_workflow (True)
        Button:
            text: "No"
            on_release: app.exec_workflow (False)
                        '''.format(del_count=delete_operation_count, end='s' if delete_operation_count > 1 else '')
            self.popup = Popup(title='Delete operation in SalesForce',
                               content=Builder.load_string(content_str),
                               size_hint=(None, None), size=(400, 150),
                               auto_dismiss=False)
            self.popup.open()
        else:
            self.exec_workflow(True)

    def exec_workflow(self, do_run_delete=True):
        if hasattr(self, 'popup'):
            self.popup.dismiss()
        connection_dict = {}
        if not do_run_delete:
            return

        for task_item in self.tasks.data:
            if task_item['exec'] and (task_item['type'] == 'SF_Query' or task_item['type'] == 'SF_Execute') and \
                    task_item['source'] not in connection_dict.keys():
                connection_dict[task_item['source']] = RESTConnector(get_conn_param(config[task_item['source']]))
        for task_item in self.tasks.data:
            cmd_exec = []
            if task_item['exec']:
                task_item['status'] = 'working'
                save_title = task_item['title']
                task_item['title'] = '{} [working]'.format(task_item['title'])
                self.refresh_tasks()
                if task_item['type'] == 'SQL_Query':
                    cmd_exec = [{
                        '': {'input_data': [input_path.strip() for input_path in task_item['input'].split(',')],
                             'sql': task_item['sql'],
                             'output_data': open(task_item['output'], 'w'),
                             'tag': None,
                             'message': ''
                             }
                    }]
                elif task_item['type'] == 'SF_Query':
                    cmd_exec = [{
                        'execute': {'input_data': task_item['sql'],
                                    'connector': connection_dict[task_item['source']],
                                    'object': self.project.get_object_from_sql(task_item['sql']).strip(),
                                    'command': 'query',
                                    'tag': None,
                                    'output_data': task_item['output'],
                                    'message': ''
                                    }
                    }]
                elif task_item['type'] == 'SF_Execute':
                    cmd_exec = [{
                        'execute': {'input_data': task_item['input'].strip(),
                                    'connector': connection_dict[task_item['source']],
                                    'object': task_item['sql'],
                                    'command': task_item['command'],
                                    'exteranl_id_name': [task_item['external_id_name']],
                                    'tag': None,
                                    'output_data': task_item['output'],
                                    'message': ''
                                    }
                    }]
                    if 'concurrency' in task_item.keys():
                        cmd_exec[0]['execute']['concurrency'] = task_item['concurrency']
                    if 'batch_size' in task_item.keys():
                        cmd_exec[0]['execute']['batch_size'] = int(task_item['batch_size'])
                elif task_item['type'] == 'MSSQL_Query':
                    with Capturing() as output:
                        timer_ = Timer()
                        try:
                            print('Execute MS SQL Query')
                            mssqlconnector = MsSQLConnector(get_conn_param(config[task_item['source']]))
                            mssqlconnector.query(task_item['sql'], task_item['output'])
                            print('Time elapsed: {0}'.format(timer_.stop()))
                        except:
                            print 'Unexpected error: ', sys.exc_info()
                            print('Time elapsed: {0}'.format(timer_.stop()))
                    for line in output:
                        if not line.startswith('Wait for job done'):
                            self.root.get_screen('tasks').ids.ti_log.text = self.root.get_screen(
                                'tasks').ids.ti_log.text + '\n' + line
                self.root.get_screen('tasks').ids.ti_log.text = self.root.get_screen(
                    'tasks').ids.ti_log.text + '\n********** {} ***********'.format(task_item['title'])
                # import threading
                with Capturing() as output:
                    try:
                        wf_task = MigrationWorkflow(src=None, dst=None, workfow=cmd_exec)
                        wf_task.execute_workflow()
                    except:
                        print "Unexpected error:", sys.exc_info()
                for line in output:
                    if not line.startswith('Wait for job done'):
                        self.root.get_screen('tasks').ids.ti_log.text = self.root.get_screen(
                            'tasks').ids.ti_log.text + '\n' + line
                task_item['status'] = 'idle'
                task_item['title'] = save_title
                self.refresh_tasks()

    def run_task__(self, cmd_exec):
        print('run in thread')
        f = open('test.txt', 'w')
        f.write('12312')
        f.close()
        try:
            wf_task = MigrationWorkflow(src=None, dst=None, workfow=cmd_exec)
            wf_task.execute_workflow()
        except:
            print "Unexpected error:", sys.exc_info()


if __name__ == '__main__':
    TaskApp(title="Mriya Query Tool").run()
