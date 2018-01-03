__author__ = 'Volodymyr'
__email__ = 'vladimir.varchuk@rackspace.com'


__author__ = 'Vladimir Varchuk'
# __all__ = ('SQLView')

from kivy.properties import NumericProperty, StringProperty, BooleanProperty, ObjectProperty, DictProperty, ListProperty
from kivy.uix.screenmanager import Screen
from kivy.uix.button import Button
from kivy.app import Builder
from os import path as p

class BatchExecuteView(Screen):
    task_index = NumericProperty()
    task_title = StringProperty()
    task_content = StringProperty()
    task_sql = StringProperty()
    task_type = StringProperty()
    task_input = StringProperty()
    task_object = StringProperty()
    task_output = StringProperty()
    task_command = StringProperty()
    task_source = StringProperty()
    task_exec = BooleanProperty()
    task_list = ListProperty ()
    task_filed_list = ObjectProperty ()
    task_ouputs_dict = DictProperty()
    selected_objects = ObjectProperty()
    preview_text = StringProperty()
    preview_source_file_name = StringProperty()


    objects_list = ListProperty()
    sources_list = ListProperty()

    previous_source = ''
    previous_sobjcet = ''



    def __init__(self, **kwargs):
        super(BatchExecuteView, self).__init__(**kwargs)
        # self.task_list.adapter.bind(on_selection_change=self.on_task_list_click)
        self.project = kwargs['project']
        self.previous_sobjcet = self.task_input
        self.previous_source = self.task_source

        input_task = [split_item<>'' and split_item  for split_item in self.task_input.split(',')]
        if self.task_input=='':
            self.selection = {}
        else:
            self.selection = {p.basename(input_task_item.lower()).split('.')[0]:[input_task_item, input_task_item.lower()] for input_task_item in input_task}
        print(self.selection)
        self.objects_list = self.project.get_standart_sobjects(self.task_source)
        # self.source_object_field_list = { field_item.lower():field_item for field_item in self.project.get_sobject_fileds(self.task_source, self.task_input)}

        if self.selection:
            self.refresh_input_task_buttons()

    def on_text_input_objects(self):
        save_inputs = self.ids.ti_input_objects.text
        inputs_list = save_inputs.split(',')
        new_selection = {}
        for input_task_item in inputs_list:
            if len(p.basename(input_task_item.lower()).split('.'))>1:
                if p.basename(input_task_item.lower()).split('.')[1] == 'csv':
                    new_selection[p.basename(input_task_item.lower()).split('.')[0]] = [input_task_item, input_task_item.lower()]
        self.selection = new_selection
        print(self.selection)
        self.refresh_input_task_buttons(refresh_source='TextInput')


    def refresh_input_task_buttons(self, refresh_source=None):
        pass
        # self.ids.st_layout.clear_widgets()
        # if self.selection:
        #     for field_item in self.selection:
        #         layout_size = self.ids.st_layout.size
        #         text_widht = len(field_item) * 9 + 20
#                 self.ids.st_layout.add_widget(Builder.load_string('''
# Button:
#     id: btn_{sql_item_lower}
#     text: '{sql_item}'
#     font_name:'Courier'
#     size_hint_y: None
#     size_hint: {x}, .15
#     on_release:root.parent.parent.parent.parent.parent.on_release_input_task_button(self.text)
# '''.format(sql_item_lower=field_item, sql_item=field_item, x=(text_widht/layout_size[0]))))
        # input_paths = []
        # selection_keys_lower = [p.basename(selection_item) for selection_item in self.selection.keys()]
        # for workflow_item in self.project.project['workflow']:
        #     if workflow_item['title'].lower() in selection_keys_lower:
        #         input_paths.append(workflow_item['output'])
        # print(input_paths)
        # if not refresh_source:
        #     self.ids.ti_input_objects.text = ', '.join([ selection_values[0] for selection_values in self.selection.values()])

    def on_task_list_click(self, d):
        if self.task_list.adapter.selection:
            if 'ctrl' in self.parent.parent.parent.parent.parent.modifiers:
                if self.task_list.adapter.selection[0].text.lower() not in self.selection.keys():
                    selection_values = []
                    for workflow_item in self.project.project['workflow']:
                        if workflow_item['title'].lower() == self.task_list.adapter.selection[0].text.lower():
                            selection_values.append(workflow_item['output'])
                            selection_values.append(workflow_item['output'].lower())
                            break
                    self.selection[self.task_list.adapter.selection[0].text.lower()] = selection_values
            self.refresh_input_task_buttons()
            fields_selected_task = []
            for tasks in self.project.project['workflow']:
                if tasks['type'] == 'SF_Query' and self.task_list.adapter.selection[0].text.lower() == tasks['title'].lower():
                    fields_selected_task = self.project.get_fields_from_sql(tasks['sql'])
                if tasks['type'] == 'SQL_Query' and self.task_list.adapter.selection[0].text.lower() == tasks['title'].lower():
                    fields_selected_task = self.project._get_fields_from_csv(tasks['output'])

            self.field_list.adapter.data = fields_selected_task
            self.field_list.adapter.bind(on_selection_change=self.on_field_list_click_item)
            if 'ctrl' in self.parent.parent.parent.parent.parent.modifiers:
                cr, cl = self.ids.ti_sql.cursor
                insert_text = 'SELECT  FROM {table_name}'.format(table_name=self.task_list.adapter.selection[0].text)
                self.ids.ti_sql.insert_text( insert_text )
                self.ids.ti_sql.cursor = (cr + 7, cl)

    def on_task_name_change(self, task_names):
        task_names_list = []
        new_task_name = self.ids.ti_task_name.text
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
        self.ids.ti_output.text = p.join(self.project.project_data_dir, self.ids.ti_task_name.text + '.csv')

    def set_task_output(self):
        self.ids.ti_task_output.text = ''

    def on_release_input_task_button(self, text):
        del self.selection[text.lower()]
        self.refresh_input_task_buttons()

    def on_field_list_click_item(self, d):
        if self.field_list.adapter.selection:
            self.ids.ti_sql.insert_text(self.field_list.adapter.selection[0].text + ',')

    def on_exec_type_text(self):
        for task in self.project.project['workflow']:
            if task['type'] == 'SQL_Query' and self.ids.ce_source_file.text.lower() == task['title'].lower():
                # fields_selected_task = self.project._get_fields_from_csv(task['output'])
                print('set input file')
                print(task['output'])
                self.load_data(task['output'])
                self.preview_source_file_name = task['output']
                self.ids.ti_text_input_source_file_name.test = task['output']



    def load_data(self, file_name):
        text = ''
        if p.exists(file_name) and self.ids.ti_lines.text.isdigit():
            f = open(file_name)
            for i in range(0, int(self.ids.ti_lines.text)):
                text = text + f.readline()
        self.preview_text = text
        self.ids.ti_source_preview.text = text

    def get_task_name_from_file(self, file_name):
        if file_name is not None and file_name <> '':
            return p.basename(file_name).split('.')[0]



    def read_controls(self, task_names):
        # self.get_task_name_string(task_names)
        if self.ids.ce_source.text <> self.previous_source:
            if self.ids.ce_source.text in self.project.get_sources() :
                self.ids.ce_object.options = [Button(text = str(x), size_hint_y=None, height=30) for x in sorted(self.project.get_standart_sobjects(self.ids.ce_source.text))]
                for option in self.ids.ce_object.options:
                    option.bind(size=option.setter('text_size'))
                self.previous_source = self.ids.ce_source.text
            else:
                self.ids.ce_object.options = []
                self.object_fileds.adapter.data = []
        # if self.ids.ce_object.text <> self.previous_sobjcet:
        #     if self.ids.ce_object.text in self.project.get_sobjects(self.ids.ce_source.text) and self.ids.ce_source.text in self.project.get_sources():
        #         self.source_object_field_list = { field_item.lower():field_item for field_item in self.project.get_sobject_fileds(self.ids.ce_source.text, self.ids.ce_object.text )}
        #         self.object_fileds.adapter.data = sorted(self.source_object_field_list.values())
        #         self.object_fileds.adapter.bind(on_selection_change=self.on_select_object_fileds_list)
        #         self.previous_sobjcet = self.ids.ce_object.text
        #     else:
        #         self.object_fileds.adapter.data = []
