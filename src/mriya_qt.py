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
from kivy.uix.textinput import TextInput
import re

from kivy.clock import Clock

Builder.load_file('mriya_qt.kv')

ObjectsList = ['Account', 'Contact', 'Opportunity', 'Rackspace_Contact__c']
FieldsListAcount = ['Id', 'CORE_Account_Number__c', 'Name', 'New_Record_id__c']
FieldsListContact = ['Id', 'AccountId', 'LastName', 'Email', 'Phone', 'DM_NewRecordId__c']
ObjectsFieldsLists = {'Account':FieldsListAcount, 'Contact':FieldsListContact}
SourceList = ['src_prod', 'dst_prod', 'src_sit', 'dst_sit']

project_dir = "C:\\Python_Projects\\mriyaQT\\data\\"

sf_object = None
sf_source = None


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

    print('TaskView class')
    def selected_object_fileds_list(self, adapter, *args):
        self.get_sql_string()

    def on_object_changed(self):
        self.get_task_name_string()
        self.get_sql_string()
        if self.ids.ce_object.text in ObjectsFieldsLists.keys():
            self.object_fileds.adapter.data = ObjectsFieldsLists[self.ids.ce_object.text]
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
                sql_string = 'SELECT {fields} FROM {object_name}'.format(fields='CHOOSE FIELDS', object_name=self.ids.ce_object.text)
        self.ids.ti_sql.text = sql_string

    def get_task_name_string(self):
        if self.ids.ce_source.text in self.sources_list and self.ids.ce_object.text in self.objects_list:
            self.ids.ti_task_name.text = self.ids.ce_source.text + '.' + self.ids.ce_object.text
        else:
            self.ids.ti_task_name.text = 'TaskName'

    def set_controls(self):
        pass

    def read_controls(self):
        self.get_task_name_string()
        pass

    def on_sql_texinput_change(self):
        sql = self.ids.ti_sql.text
        res_re = re.search('SELECT(.*?)FROM',sql, re.IGNORECASE)
        print(res_re.groups())
        if len(res_re.groups()) > 0:
            fields_list = res_re.group(1).strip().split(',')
            print(fields_list)
            for field_item in fields_list:
                print(field_item)
                if field_item in ObjectsFieldsLists[self.ids.ce_object.text]:
                    print('find')
                    self.object_fileds.handle_selection( self.object_fileds.get_data_item(ObjectsFieldsLists[self.ids.ce_object.text].index(field_item)) , True)

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
        self.go_tasks()

    def edit_task(self, task_index):
        print('TaskApp edit task')
        task = self.tasks.data[task_index]
        name = 'task{}'.format(task_index)

        if self.root.has_screen(name):
            self.root.remove_widget(self.root.get_screen(name))

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
        self.root.add_widget(view)
        self.transition.direction = 'left'
        self.root.current = view.name

    def add_task(self):
        print('TaskApp add task')
        self.tasks.data.append({'title': 'New task', 'content': '', 'sql':'', 'type':'', 'input':'', 'output':'', 'source':''})
        task_index = len(self.tasks.data) - 1
        self.edit_task(task_index)

    def set_task_content(self, task_index, task_content):
        print('TaskApp set task content')
        self.tasks.data[task_index]['content'] = task_content
        data = self.tasks.data
        self.tasks.data = []
        self.tasks.data = data
        self.save_tasks()
        self.refresh_tasks()

    def set_task_title(self, task_index, task_title):
        print('TaskApp set task title')
        self.tasks.data[task_index]['title'] = task_title
        self.save_tasks()
        self.refresh_tasks()

    def set_task_sql(self, task_index, task_sql):
        print('TaskApp set task sql')
        self.tasks.data[task_index]['sql'] = task_sql
        self.save_tasks()
        self.refresh_tasks()

    def set_task_type(self, task_index, task_type):
        print('TaskApp set task title')
        self.tasks.data[task_index]['type'] = task_type
        self.save_tasks()
        self.refresh_tasks()

    def set_task_input(self, task_index, task_input):
        print('TaskApp set task input')
        self.tasks.data[task_index]['input'] = task_input
        self.save_tasks()
        self.refresh_tasks()

    def set_task_output(self, task_index, task_output):
        print('TaskApp set task title')
        self.tasks.data[task_index]['output'] = task_output
        self.save_tasks()
        self.refresh_tasks()

    def set_task_source(self, task_index, task_source):
        print('TaskApp set task title')
        self.tasks.data[task_index]['source'] = task_source
        self.save_tasks()
        self.refresh_tasks()

    def refresh_tasks(self):
        print('TaskApp refresh')
        data = self.tasks.data
        self.tasks.data = []
        self.tasks.data = data

    def go_tasks(self, task_item):
        print('TaskApp on tasks')
        print(task_item)
        print(self.tasks.data)
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

    @property
    def tasks_fn(self):
        print(join(project_dir, 'tasks.json'))
        return join(project_dir, 'tasks.json')

if __name__ == '__main__':
    TaskApp().run()
