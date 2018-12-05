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
    task_external_id_name = StringProperty()
    selected_objects = ObjectProperty()
    preview_text = StringProperty()
    preview_source_file_name = StringProperty()
    task_batch_size = StringProperty()
    task_concurrency = StringProperty()

    objects_list = ListProperty()
    sources_list = ListProperty()

    # previous_source = ''
    # previous_sobjcet = ''

    def __init__(self, **kwargs):
        self.project = kwargs['project']
        super(BatchExecuteView, self).__init__(**kwargs)
        self.previous_source = self.task_source
        self.objects_list = self.project.get_standart_sobjects(self.task_source)

    def on_text_input_objects(self):
        save_inputs = self.ids.ti_input_objects.text
        inputs_list = save_inputs.split(',')
        new_selection = {}
        for input_task_item in inputs_list:
            if len(p.basename(input_task_item.lower()).split('.'))>1:
                if p.basename(input_task_item.lower()).split('.')[1] == 'csv':
                    new_selection[p.basename(input_task_item.lower()).split('.')[0]] = [input_task_item, input_task_item.lower()]
        self.selection = new_selection
        self.refresh_input_task_buttons(refresh_source='TextInput')

    def get_csv_fields(self, file_name):
        return self.project._get_fields_from_csv(file_name)


    def refresh_input_task_buttons(self, refresh_source=None):
        pass

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
        pass

    def refresh_fileds_buttons(self, d):
            self.ids.st_layout.clear_widgets()
            if self.selection:
                for field_item in self.selection.values():
                    layout_size = self.ids.st_layout.size
                    text_widht = len(field_item) * 9 + 20

                    self.ids.st_layout.add_widget(Builder.load_string('''
    Button:
        id: btn_{sql_item_lower}
        text: '{sql_item}'
        font_name:'Courier'
        size_hint_y: None
        size_hint: {x}, .15
        on_release:root.parent.parent.parent.parent.parent.on_release_field_button(self.text)
    '''.format(sql_item_lower=field_item.lower(), sql_item=field_item, x=(text_widht / layout_size[0]))))

    def on_exec_type_text(self):
        for task in self.project.project['workflow']:
            if task['type'] == 'SQL_Query' and self.ids.ce_source_file.text.lower() == task['title'].lower():
                self.ids.ti_task_name.text = self.ids.ce_source_file.text.lower() + '_' + self.ids.ce_exec_type.text
                self.load_data(task['output'])
                self.preview_source_file_name = task['output']
                self.ids.ti_text_input_source_file_name.text = task['output']
                if self.ids.ce_exec_type.text == 'insert':
                    self.ids.ce_external_id_name.options = [Button(text = str(x), size_hint_y=None, height=30) for x in sorted(self.project._get_fields_from_csv(task['output']))]
                else:
                    self.ids.ce_external_id_name.text = 'Id'

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
        if self.ids.ce_source.text <> self.previous_source:
            if self.ids.ce_source.text in self.project.get_sources() :
                self.ids.ce_object.options = [Button(text = str(x), size_hint_y=None, height=30) for x in sorted(self.project.get_standart_sobjects(self.ids.ce_source.text))]
                for option in self.ids.ce_object.options:
                    option.bind(size=option.setter('text_size'))
                self.previous_source = self.ids.ce_source.text
            else:
                self.ids.ce_object.options = []
                self.object_fileds.adapter.data = []
