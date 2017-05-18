__author__ = 'Volodymyr Varchuk'
__email__ = 'vladimir.varchuk@rackspace.com'

'''
Mriya Query Tool
================

Application for extracting and querying SalesForce data using SQL (SQLite engine)

'''

__version__ = '1.0'

import json
from os.path import join, exists
from kivy.app import App, Builder
from kivy.uix.screenmanager import ScreenManager, Screen, SlideTransition
from kivy.properties import ListProperty, StringProperty, \
    NumericProperty, ObjectProperty, BooleanProperty
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.floatlayout import FloatLayout
from kivy.uix.dropdown import DropDown
from kivy.uix.button import Button
from kivy.uix.textinput import TextInput
from kivy.uix.popup import Popup
from kivy.uix.label import Label
from data_connector import RESTConnector, SFBeatboxConnector
from configparser import ConfigParser
from data_connector import get_conn_param
import re
from os import path as p

from kivy.clock import Clock

Builder.load_file('mriya_qt.kv')

# ObjectsList = ['Account', 'Contact', 'Opportunity', 'Rackspace_Contact__c']
FieldsListAcount = ['Id', 'CORE_Account_Number__c', 'Name', 'New_Record_id__c']
FieldsListContact = ['Id', 'AccountId', 'LastName', 'Email', 'Phone', 'DM_NewRecordId__c']
ObjectsFieldsLists = {'Account':FieldsListAcount, 'Contact':FieldsListContact}


# project_dir = "/home/volodymyr/git/mriyaQT/data/"
project_dir = "C:\\Python_Projects\\mriyaQT\\data\\"
config_file = "config.ini"


sf_object = None
sf_source = None

config = ConfigParser()
with open(config_file, 'r') as conf_file:
   config.read_file(conf_file)

SourceList = config.keys()[1:]

def get_sobjects(connection_name):
    bb = SFBeatboxConnector(get_conn_param(config[connection_name]))
    sobjects = bb.svc.describeTabs()
    tab_list = []
    for tab in sobjects:
        for dic_tab in tab:
            if type(tab[dic_tab]) is list:
                for list_tab in tab[dic_tab]:
                    if list_tab['sObjectName'] not in tab_list and list_tab['sObjectName'] != '':
                        tab_list.append(list_tab['sObjectName'])
    # print(bb.svc.describeSObjects('Account')[0].fields)
    return sorted(tab_list)

def get_field_list(object_name, connection):
    print(object_name)
    print(connection)
    print('!!!!!!!!!!!!!!!!!!!!!!!')
    if connection in SourceList:
        if object_name in ObjectsList:
            bb = SFBeatboxConnector(get_conn_param(config[connection]))
            fields = bb.svc.describeSObjects(object_name)[0].fields.keys()
            print(fields)
            return sorted(fields)
    return []

ObjectsList = get_sobjects('dst_sit')

class ComboEdit(TextInput):
    options = ListProperty(('', ))
    def __init__(self, **kw):
        ddn = self.drop_down = DropDown()
        ddn.bind(on_select=self.on_select)
        super(ComboEdit, self).__init__(**kw)
        print(self.options)

    def on_options(self, instance, value):
        print('combo on optios')
        ddn = self.drop_down
        ddn.clear_widgets()
        for widg in value:
            widg.bind(on_release=lambda btn: ddn.select(btn.text))
            ddn.add_widget(widg)

    def on_select(self, *args):
        print('onselect')
        print(args[1])
        self.text = args[1]

    def on_touch_up(self, touch):
        if touch.grab_current == self:
            self.drop_down.open(self)
        return super(ComboEdit, self).on_touch_up(touch)


class TaskView(Screen):
    task_index = NumericProperty()
    task_title = StringProperty()
    task_content = StringProperty()
    task_sql = StringProperty()
    task_type = StringProperty()
    task_input = StringProperty()
    task_output = StringProperty()
    task_source = StringProperty()

    object_fileds = ObjectProperty ()
    objects_list = ObjectProperty(ObjectsList)
    sources_list = ObjectProperty(SourceList)
    object_selected = False

    source_object_list = []
    source_object_field_list = []

    print('TaskView class')
    def selected_object_fileds_list(self, adapter, *args):
        self.get_sql_string()

    def on_object_changed(self, task_names):
        self.get_task_name_string(task_names)
        self.get_sql_string()
        if self.ids.ce_object.text in ObjectsList:
            # ObjectsFieldsLists.keys():
            self.object_fileds.adapter.data = self.get_fields_list()
            # ObjectsFieldsLists[self.ids.ce_object.text]
        else:
            print('Load new object')
            self.object_fileds.adapter.data = []
        self.object_fileds.adapter.bind(on_selection_change=self.selected_object_fileds_list)
        self.object_selected = True
        print('got it')

    def get_sql_string(self):
        sql_string = 'First select object from list'
        if self.ids.ce_object.text in self.objects_list:
            if self.object_fileds.adapter.selection:
                sql_string = 'SELECT {fields} FROM {object_name}'.format(fields=', '.join([selected_item.text for selected_item in self.object_fileds.adapter.selection]), object_name=self.ids.ce_object.text)
            else:
                sql_string = 'SELECT {fields} FROM {object_name}'.format(fields=' ', object_name=self.ids.ce_object.text)
        self.ids.ti_sql.text = sql_string

    def get_task_name_string(self, task_names):
        if self.ids.ce_source.text in self.sources_list and self.ids.ce_object.text in self.objects_list:
            new_task_name = self.ids.ce_source.text + '.' + self.ids.ce_object.text
        else:
            new_task_name = 'TaskName'
        same_task_name_index = 0
        for i, task_name in enumerate(task_names):
            if task_name.startswith(new_task_name) and self.task_index != i:
                if new_task_name != task_name:
                    suffix = task_name[len(new_task_name):]
                    if suffix.isdigit():
                        same_task_name_index = int(suffix) + 1
                else:
                    same_task_name_index = 1
        if same_task_name_index == 0:
            self.ids.ti_task_name.text = new_task_name
        else:
            self.ids.ti_task_name.text = '{0}{1:02d}'.format(new_task_name, same_task_name_index)
        self.ids.ti_output.text = p.join(project_dir, self.ids.ti_task_name.text + '.csv')

    def set_controls(self):
        pass

    def read_controls(self, task_names):
        self.get_task_name_string(task_names)
        if self.ids.ce_source.text in SourceList:
            self.source_object_list = self.get_objects_list_()
        self.ids.ce_object.options = [Button(text = str(x), size_hint_y=None, height=30) for x in self.source_object_list]
        for option in self.ids.ce_object.options:
            option.bind(size=option.setter('text_size'))
        if self.ids.ce_object.text in self.source_object_list:
            self.source_object_field_list = self.get_fields_list()

        self.object_fileds.adapter.data = self.source_object_field_list

    def on_task_title_changed(self, task_names):
        self.get_task_name_string(task_names)

    def on_sql_texinput_change(self):
        pass
        # sql = self.ids.ti_sql.text
        # res_re = re.search('SELECT(.*?)FROM',sql, re.IGNORECASE)
        # print(res_re.groups())
        # if len(res_re.groups()) > 0:
        #     fields_list = res_re.group(1).strip().split(',')
        #     print(fields_list)
        #     for field_item in fields_list:
        #         print(field_item)
        #         if field_item in ObjectsFieldsLists[self.ids.ce_object.text]:
        #             print('find')
        #             self.object_fileds.handle_selection( self.object_fileds.get_data_item(ObjectsFieldsLists[self.ids.ce_object.text].index(field_item)) , True)

    def exec_item(self, task_index):
        print(task_index)

    def get_fields_list(self):
        return get_field_list(self.ids.ce_object.text, self.ids.ce_source.text)

    def get_objects_list_(self):
        return get_sobjects(self.ids.ce_source.text)

class SQLView(Screen):
    task_index = NumericProperty()
    task_title = StringProperty()
    task_content = StringProperty()
    task_sql = StringProperty()
    task_type = StringProperty()
    task_input = StringProperty()
    task_output = StringProperty()
    task_source = StringProperty()

    object_fileds = ObjectProperty ()
    objects_list = ObjectProperty(['test'])
    sources_list = ObjectProperty(['aaa'])
    object_selected = False
    source_object_list = []
    source_object_field_list = []


class TaskListItem(BoxLayout):
    task_content = StringProperty()
    task_title = StringProperty()
    task_index = NumericProperty()
    task_sql = StringProperty()
    task_type = StringProperty()
    task_input = StringProperty()
    task_output = StringProperty()
    task_source = StringProperty()

    def __init__(self, **kwargs):
        print('TaskListItem init')
        print(kwargs)
        del kwargs['index']
        super(TaskListItem, self).__init__(**kwargs)


class Tasks(Screen):
    data = ListProperty()

    def args_converter(self, row_index, item):
        print(item)
        return {
            'task_index': row_index,
            'task_content': item['content'],
            'task_title': item['title'],
            'task_sql':item['sql'],
            'task_type':item['type'],
            'task_input':item['input'],
            'task_output':item['output'],
            'task_source':item['source']
        }

class TaskApp(App):
    def build(self):
        print('TaskApp build')
        self.tasks = Tasks(name='tasks')
        self.load_tasks()

        self.transition = SlideTransition(duration=.35)
        root = ScreenManager(transition=self.transition)
        root.add_widget(self.tasks)
        return root

    def load_tasks(self):
        print('TaskApp load tasks')
        if not exists(self.tasks_fn):
            return
        with open(self.tasks_fn) as fd:
            data = json.load(fd)
        self.tasks.data = data

    def save_tasks(self):
        print('TaskApp save tasks')
        with open(self.tasks_fn, 'w') as fd:
            json.dump(self.tasks.data, fd)

    def del_task(self, task_index):
        print('TaskApp del task')
        del self.tasks.data[task_index]
        self.save_tasks()
        self.refresh_tasks()
        self.go_tasks(task_index)

    def edit_task(self, task_index):
        print('TaskApp edit task')
        task = self.tasks.data[task_index]
        name = 'task{}'.format(task_index)

        if self.root.has_screen(name):
            self.root.remove_widget(self.root.get_screen(name))

        print(task.get('type'))
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
                task_source=task.get('source')
            )
        elif task.get('type') == 'SQL_Query':
            print('sql query')
            print(task)
            view = SQLView(
                name=name,
                task_index=task_index,
                task_title=task.get('title'),
                task_content=task.get('content'),
                task_sql=task.get('sql'),
                task_type=task.get('type'),
                task_input=task.get('input'),
                task_output=task.get('output'),
                task_source=task.get('source')
            )
        print(view)
        print(view.name)
        self.root.add_widget(view)
        self.transition.direction = 'left'
        self.root.current = view.name

    def add_task(self, task_type):
        print('TaskApp add task')
        self.tasks.data.append({'title': 'NewTask', 'content': '', 'sql':'', 'type':task_type, 'input':'', 'output':'', 'source':''})
        task_index = len(self.tasks.data) - 1
        print('go to edit task')
        self.edit_task(task_index)

    def refresh_tasks(self):
        print('TaskApp refresh')
        data = self.tasks.data
        self.tasks.data = []
        self.tasks.data = data

    def go_tasks(self, task_item):
        print('TaskApp on tasks')
        print(task_item)
        print(self.tasks.data)
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

    def exec_task(self, task_index):
        print('executing sql {}'.format(self.tasks.data[task_index]['title']))
        self.save_tasks()
        self.refresh_tasks()

    def get_task_names(self):
        return [task_item['title'] for task_item in self.tasks.data]

    def save_task(self, task_index, data):
        print(self.tasks.data)
        print(data)
        for data_item in data:
            self.tasks.data[task_index][data_item] = data[data_item]
        self.save_tasks()

    def get_object_from_sql(self, soql):
        res = None
        try:
            res = re.search('FROM(.*?)WHERE', soql, re.IGNORECASE).group(1).strip()
        except:
            try:
                res = re.search('FROM(.*?)LIMIT', soql, re.IGNORECASE).group(1).strip()
            except:
                try:
                    res = re.search('FROM(.*?)GROUP', soql,
                                    re.IGNORECASE).group(1).strip()
                except:
                    try:
                        res = re.search('FROM(.*?)ORDER', soql,
                                        re.IGNORECASE).group(1).strip()
                    except:
                        res = soql[soql.find('FROM') + 5:].strip()
        return res

    def get_objects_list(self, connection):
        return get_sobjects(connection)

    def get_field_list(self, object_name, connection):
        return get_field_list(object_name, connection)

    @property
    def tasks_fn(self):
        print(join(project_dir, 'tasks.json'))
        return join(project_dir, 'tasks.json')

if __name__ == '__main__':
    TaskApp().run()
