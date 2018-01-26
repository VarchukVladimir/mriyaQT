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

import json
import re
import sys
from os import path as p
from os import mkdir, getcwd

from kivy.app import App, Builder
from kivy.uix.screenmanager import ScreenManager, Screen, SlideTransition
from kivy.properties import ListProperty, StringProperty, \
    NumericProperty, BooleanProperty
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.dropdown import DropDown
from kivy.uix.textinput import TextInput

from data_connector import RESTConnector, SFBeatboxConnector
from configparser import ConfigParser
from data_connector import get_conn_param
from migration_engine import MigrationWorkflow
from sql_view import SQLView
from sf_view import TaskView
from start_screen import StartScreen
from review_screen import ReviewScreen
from sf_execute_view import BatchExecuteView
from project_utils import Capturing


default_project_dir = p.join('/home/volodymyr/work', 'test_exec', 'test_exec.mpr')
# default_project_dir = p.join(getcwd(), 'projects', 'test_exec', 'test_exec.mpr')
default_application_dir = p.join(p.expanduser('~'), 'work')
Builder.load_file('mriya_qt.kv')

standart_object_list_file = p.join('StandartObjectList.ini')

StandartObjectList = ['Account', 'Contact', 'AccountCaontactRole', 'Lead', 'Opportunity', 'Task', 'Events', 'Notes', 'Attachment',
    'Campaign', 'CampaignMember', 'User', 'AccountTeamMember']

if p.exists(standart_object_list_file):
    jj = json.load(open(standart_object_list_file))
    StandartObjectList = jj[0]['StandartObjectList']

StandartObjectList_uppercase = [ object_item.upper() for object_item in StandartObjectList]

ObjectsList = []

config_file = p.join("config.ini")

sf_object = None
sf_source = None

config = ConfigParser()
with open(config_file, 'r') as conf_file:
   config.read_file(conf_file)
SourceList = config.keys()[1:]
ObjectsMetadata = []

def get_fields_rest(connection, sobject):
    rc = RESTConnector(get_conn_param(config[connection]))
    req_res = json.loads('[{}]'.format(rc.bulk.rest_request('sobjects/{}/describe'.format(sobject))))
    fields = []
    for field_item in req_res[0]['fields']:
        fields.append(field_item['name'])
    return sorted(fields)

class Project():
    def __init__(self, file_name = default_project_dir):
        if not p.exists(file_name):
            mkdir(file_name)
            mkdir( p.join(file_name, 'data'))
            self.project_file_name = p.join(p.join( p.dirname(file_name), p.basename(file_name)), p.basename(file_name) + '.mpr')
            self.project_dir = file_name
            self.project_data_dir = p.join(self.project_dir, 'data')
            self.project_name = p.basename(file_name)
            self.project = {
                'metadata':{
                    source_name:{} for source_name in config.keys()},
                'workflow':[],
                'project_name': self.project_name ,
                'project_data_dir': self.project_data_dir,
                'project_dir': self.project_dir
            }
        else:
            self.project_file_name = file_name
            self.project = json.load(open(file_name))
            self.project_dir = self.project['project_dir']
            self.project_data_dir = self.project['project_data_dir']
            self.project_name = self.project['project_name']
            for source_item in SourceList:
                if source_item not in self.project['metadata']:
                    self.project['metadata'][source_item]  = {}

    def save(self):
        json.dump(self.project, open(self.project_file_name, 'w'))

    def get_sobjects(self, connection, force_refresh = False):
        if connection not in SourceList:
            return []
        if self.project['metadata'][connection] == {} or force_refresh:
            rest_connector = RESTConnector(get_conn_param(config[connection]))
            req_res = json.loads('[{}]'.format(rest_connector.bulk.rest_request('sobjects')))[0]['sobjects']
            self.project['metadata'][connection] = { (type(sobject['name']) is str or type(sobject['name']) is unicode) and sobject['name']:{} for sobject in req_res}
        return sorted(self.project['metadata'][connection].keys())

    def get_sobject_fileds(self, connection, sobject, force_refresh=False):
        if self.project['metadata'][connection] == {}:
            self.get_sobjects(connection)
        if force_refresh or self.project['metadata'][connection][sobject] == {}:
            rest_connector = RESTConnector(get_conn_param(config[connection]))
            req_res = json.loads('[{}]'.format(rest_connector.bulk.rest_request('sobjects/{}/describe'.format(sobject))))
            fields = []
            for field_item in req_res[0]['fields']:
                fields.append(field_item['name'])
            self.project['metadata'][connection][sobject] = sorted(fields)
        return sorted(self.project['metadata'][connection][sobject])

    def get_standart_sobjects(self, connection, force_refresh = False):
        objects_list = self.get_sobjects(connection, force_refresh=force_refresh)
        show_object_list = []

        for object_item in objects_list:
            if object_item.upper() in StandartObjectList_uppercase:
                show_object_list.append(object_item)
        return show_object_list

    def get_sources(self):
        return SourceList

    def get_object_from_sql(self, soql):
        res = None
        soql_upper = soql
        try:
            res = re.search('FROM(.*?)WHERE', soql_upper, re.IGNORECASE).group(1).strip()
        except:
            try:
                res = re.search('FROM(.*?)LIMIT', soql_upper, re.IGNORECASE).group(1).strip()
            except:
                try:
                    res = re.search('FROM(.*?)GROUP', soql_upper,
                                    re.IGNORECASE).group(1).strip()
                except:
                    try:
                        res = re.search('FROM(.*?)ORDER', soql_upper,
                                        re.IGNORECASE).group(1).strip()
                    except:
                        res = soql[soql_upper.find('FROM') + 5:].strip()
        return res

    def get_sql_after_from(self, soql):
        soql_upper = soql.upper()
        after_from = soql[soql_upper.find('FROM'):].strip().split(' ')
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
                field_names = f.readline().replace('"','').strip().split(',')
        return field_names


class ComboEdit(TextInput):
    options = ListProperty(('', ))
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
    task_sql = StringProperty()
    task_type = StringProperty()
    # task_command = StringProperty()
    task_input = StringProperty()
    task_output = StringProperty()
    task_source = StringProperty()
    task_exec = BooleanProperty()
    task_status = StringProperty()
    def __init__(self, **kwargs):
        print('TaskListItem init')
        print(kwargs)
        del kwargs['index']
        print(kwargs)
        super(TaskListItem, self).__init__(**kwargs)

    def refresh_status(self):
        # self.ids.label_task_name.background_color
        if self.task_status == 'working':
            print(self.task_status)

class Tasks(Screen):
    data = ListProperty()
    def args_converter(self, row_index, item):
        # if item is SF_Execute type it may have command key
        if 'command' in item.keys():
            return {
                'task_index': row_index,
                'task_content': item['content'],
                'task_title': item['title'],
                'task_sql':item['sql'],
                'task_type':item['type'],
                'task_command':item['command'],
                'task_input':item['input'],
                'task_output':item['output'],
                'task_source':item['source'],
                'task_exec':item['exec'],
                'task_status':item['status']
            }
        else:
            return {
                'task_index': row_index,
                'task_content': item['content'],
                'task_title': item['title'],
                'task_sql':item['sql'],
                'task_type':item['type'],
                'task_input':item['input'],
                'task_output':item['output'],
                'task_source':item['source'],
                'task_exec':item['exec'],
                'task_status':item['status']
            }


class TaskApp(App):

    def __init__(self, **kwargs):
        super(TaskApp, self).__init__(**kwargs)
        self.default_project_dir = default_project_dir
        self.default_application_dir = default_application_dir

    def build(self):
        self.start_screen = StartScreen()
        self.transition = SlideTransition(duration=.35)
        root = ScreenManager(transition=self.transition)
        root.add_widget(self.start_screen)
        return root

    def on_release_field_button(self, text):
        print(text)

    def load_tasks(self):
        data = self.project.project['workflow']
        self.tasks.data = data

    def save_tasks(self):
        self.project.project['workflow'] = self.tasks.data
        self.project.save()

    def del_task(self, task_index):
        del self.tasks.data[task_index]
        self.save_tasks()
        self.refresh_tasks()
        self.go_tasks(task_index)

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
                project = self.project,
                sources_list = SourceList
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
            view.task_ouputs_dict = {task_name['title']:task_name['output'] for task_name in self.tasks.data[:task_index]}
        elif task.get('type') == 'SF_Execute':
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
                sources_list = SourceList,
                preview_text = '',
                project=self.project
            )
            view.task_list = [task_name['title'] for task_name in self.tasks.data[:task_index]]
            view.task_ouputs_dict = {task_name['title']:task_name['output'] for task_name in self.tasks.data[:task_index]}
        self.root.add_widget(view)
        self.transition.direction = 'left'
        self.root.current = view.name

    def add_task(self, task_type):
        task_names_index = []
        projectname = self.project.project_name
        for task_name_item in [ task_item['title'] for task_item in self.tasks.data ]:
            if task_name_item.startswith(projectname) and task_name_item[len(projectname):].isdigit():
                task_names_index.append(task_name_item)
        max_index = 1
        if task_names_index:
            max_index = int(max(task_names_index)[len(projectname):]) + 1
        new_title_name = '{0}{1:02d}'.format(projectname, max_index)
        self.tasks.data.append({'title': new_title_name, 'content': '', 'sql':'', 'type':task_type, 'input':' ', 'output': p.join(self.project.project_data_dir, new_title_name + '.csv') , 'source':'', 'exec':False, 'status':'idle', 'command':'update'})
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
            self.tasks.data[task_index], self.tasks.data[task_index - 1] = self.tasks.data[task_index - 1], self.tasks.data[task_index]
        self.save_tasks()
        self.refresh_tasks()

    def down_task(self, task_index):
        if task_index != len(self.tasks.data) - 1:
            self.tasks.data[task_index], self.tasks.data[task_index + 1] = self.tasks.data[task_index + 1], self.tasks.data[task_index]
        self.save_tasks()
        self.refresh_tasks()

    def get_task_names(self):
        return [task_item['title'] for task_item in self.tasks.data]

    def save_task(self, task_index, data):
        for data_item in data:
            self.tasks.data[task_index][data_item] = data[data_item]
        self.save_tasks()

    def on_skip_task(self, instance, value, task_index):
        self.tasks.data[task_index]['exec'] = value

    def go_to_project(self, prject_file):
        self.project = Project(prject_file)
        self.tasks = Tasks(name='tasks')
        self.load_tasks()
        self.root.add_widget(self.tasks)
        self.transition.direction = 'left'
        self.root.current = self.tasks.name

    def go_to_review_screen(self, output_path):
        review_screen = ReviewScreen(name=p.basename(output_path), file_name=output_path, previous_screen=self.root.current)
        if self.root.has_screen(review_screen.name):
            self.root.remove_widget(self.root.get_screen(review_screen.name))
        self.root.add_widget(review_screen)
        self.transition.direction = 'left'
        self.root.current = review_screen.name

    def go_back_to_task(self, previous_screen):
        self.transition.direction = 'right'
        self.root.current = previous_screen

    def exec_workflow(self):
        connection_dict = {}
        for task_item in self.tasks.data:
            if task_item['exec'] and  (task_item['type'] == 'SF_Query' or task_item['type'] == 'SF_Execute') and task_item['source'] not in connection_dict.keys():
                connection_dict[task_item['source']] = RESTConnector(get_conn_param(config[task_item['source']]))

        for task_item in self.tasks.data:
            cmd_exec = []
            if task_item['exec']:
                task_item['status'] = 'working'
                save_title = task_item['title']
                task_item['title'] =  '{} [working]'.format(task_item['title'])
                self.refresh_tasks()
                if task_item['type'] == 'SQL_Query':
                    cmd_exec = [{
                        '':{'input_data':[ input_path.strip() for input_path in task_item['input'].split(',')],
                                'sql':task_item['sql'],
                                'output_data':open(task_item['output'], 'w'),
                                'tag':None,
                                'message':''
                                }
                    }]
                elif task_item['type'] == 'SF_Query':
                    cmd_exec = [{
                    'execute':{'input_data':task_item['sql'],
                               'connector':connection_dict[task_item['source']],
                               'object':self.project.get_object_from_sql(task_item['sql']),
                               'command':'query',
                               'tag':None,
                               'output_data':task_item['output'],
                               'message':''
                               }
                    }]
                elif task_item['type'] == 'SF_Execute':
                    # print(connection_dict)
                 # {'sql':'', 'source':ce_source.text, 'type':'SF_Execute', 'output':ti_output.text, 'input':ti_text_input_source_file_name.text, 'title':ti_task_name.text, 'exec' : cb_run_in_workflow.active, 'command':ce_exec_type.text}

                    cmd_exec = [{
                    'execute':{'input_data':task_item['input'],
                               'connector':connection_dict[task_item['source']],
                               'object':'Account',
                               'command':task_item['command'],
                               'tag':None,
                               'output_data':task_item['output'],
                               'message':''
                               }
                    }]
                self.root.get_screen('tasks').ids.ti_log.text = self.root.get_screen('tasks').ids.ti_log.text +'\n********** {} ***********'.format(task_item['title'])
                print(cmd_exec)
                with Capturing() as output:
                    try:
                        wf_task = MigrationWorkflow(src=None, dst=None, workfow=cmd_exec)
                        wf_task.execute_workflow()
                    except:
                        print "Unexpected error:", sys.exc_info()
                for line in output:
                    if not line.startswith('Wait for job done'):
                        self.root.get_screen('tasks').ids.ti_log.text = self.root.get_screen('tasks').ids.ti_log.text +'\n'+ line
                task_item['status'] = 'idle'
                task_item['title'] = save_title
                self.refresh_tasks()
                # self.tasks.ids.lv_tasks._trigger_reset_populate()
#

if __name__ == '__main__':
    TaskApp(title="Mriya Query Tool").run()
