__author__ = 'Vladimir Varchuk'

from kivy.properties import NumericProperty, StringProperty, BooleanProperty, ObjectProperty, ListProperty
from kivy.uix.screenmanager import Screen
from kivy.app import Builder
from kivy.uix.button import Button
from os import path as p

class TaskView(Screen):
    task_index = NumericProperty()
    task_title = StringProperty()
    task_content = StringProperty()
    task_sql = StringProperty()
    task_type = StringProperty()
    task_input = StringProperty()
    task_output = StringProperty()
    task_source = StringProperty()
    task_exec = BooleanProperty()

    object_fileds = ObjectProperty ()
    selected_fileds = ObjectProperty ()
    objects_list = ListProperty()
    sources_list = ListProperty()

    previous_source = ''
    previous_sobjcet = ''

    def __init__(self, **kwargs):
        super(TaskView, self).__init__(**kwargs)
        self.project = kwargs['project']
        self.task_input = self.project.get_object_from_sql(self.task_sql)
        self.previous_sobjcet = self.task_input
        self.previous_source = self.task_source
        self.source_object_field_list = {}
        self.selection = {}
        self.pressed_filed_list_button = ''
        self.set_not_to_refresh = True
        self.objects_list = self.project.get_standart_sobjects(self.task_source)
        if self.task_source in self.sources_list and self.task_input in self.project.get_sobjects(self.task_source):
            self.source_object_field_list = { field_item.lower():field_item for field_item in self.project.get_sobject_fileds(self.task_source, self.task_input)}
            self.object_fileds.adapter.data = self.project.get_sobject_fileds(self.task_source, self.task_input)
            self.on_sql_texinput_change()
            self.refresh_fileds_buttons()

    def on_select_object_fileds_list(self, adapter, *args):
        if self.object_fileds.adapter.selection:
            if self.object_fileds.adapter.selection[0].text.lower() not in self.selection.keys():
                self.selection[self.object_fileds.adapter.selection[0].text.lower()] = self.object_fileds.adapter.selection[0].text
        self.get_sql_string()

    def get_sql_string(self):
        sql_string = ''
        save_sql = self.ids.ti_sql.text
        save_where = self.project.get_sql_after_from(save_sql)
        if self.ids.ce_object.text in self.project.get_sobjects(self.ids.ce_source.text):
            if self.selection:
                sql_string = 'SELECT {fields} FROM {object_name} {after_where}'.format(fields=', '.join([self.selection[selected_item] for selected_item in self.selection]), object_name=self.ids.ce_object.text, after_where=save_where)
            else:
                sql_string = 'SELECT {fields} FROM {object_name} {after_where}'.format(fields=' ', object_name=self.ids.ce_object.text, after_where=save_where)
        self.ids.ti_sql.text = sql_string

    def get_task_name_string(self, task_names):
        if self.ids.ce_source.text in self.project.get_sources() and self.ids.ce_object.text in self.project.get_sobjects(self.ids.ce_source.text):
            new_task_name = self.ids.ce_source.text + '_' + self.ids.ce_object.text
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
        self.ids.ti_output.text = p.join(self.project.project_data_dir, self.ids.ti_task_name.text + '.csv')

    def read_controls(self, task_names):
        self.get_task_name_string(task_names)
        if self.ids.ce_source.text <> self.previous_source:
            if self.ids.ce_source.text in self.project.get_sources() :
                self.ids.ce_object.options = [Button(text = str(x), size_hint_y=None, height=30) for x in sorted(self.project.get_standart_sobjects(self.ids.ce_source.text))]
                for option in self.ids.ce_object.options:
                    option.bind(size=option.setter('text_size'))
                self.previous_source = self.ids.ce_source.text
            else:
                self.ids.ce_object.options = []
                self.object_fileds.adapter.data = []
        if self.ids.ce_object.text <> self.previous_sobjcet:
            if self.ids.ce_object.text in self.project.get_sobjects(self.ids.ce_source.text) and self.ids.ce_source.text in self.project.get_sources():
                self.source_object_field_list = { field_item.lower():field_item for field_item in self.project.get_sobject_fileds(self.ids.ce_source.text, self.ids.ce_object.text )}
                self.object_fileds.adapter.data = sorted(self.source_object_field_list.values())
                self.object_fileds.adapter.bind(on_selection_change=self.on_select_object_fileds_list)
                self.previous_sobjcet = self.ids.ce_object.text
            else:
                self.object_fileds.adapter.data = []

    def on_task_title_changed(self, task_names):
        self.get_task_name_string(task_names)

    def exec_item(self, task_index):
        print(task_index)

    def on_text_field_filter(self, input_text):
        if self.source_object_field_list:
            print(self.object_fileds)
            selection = self.object_fileds.adapter.selection
            self.object_fileds.adapter.data = []
            for field_item in sorted(self.source_object_field_list.values()):
                if field_item.lower().startswith(input_text.lower()):
                    self.object_fileds.adapter.data.append(field_item)
            self.object_fileds.adapter.bind(on_selection_change=self.on_select_object_fileds_list)

    def on_press_field_button(self, text):
        if not self.set_not_to_refresh:
            self.pressed_filed_list_button = text.lower()
        else:
            self.set_not_to_refresh = False
            self.pressed_filed_list_button = ''

    def on_release_field_button(self, text):
        pass
        # if text.lower() == self.pressed_filed_list_button:
        #     print('deleting {}'.format(text))
        #     # del self.selection[text.lower()]
        #     self.refresh_fileds_buttons()
        #     self.get_sql_string()
        # self.pressed_filed_list_button = ''

    def refresh_fileds_buttons(self):
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
    on_press:root.parent.parent.parent.parent.parent.on_press_field_button(self.text)
'''.format(sql_item_lower=field_item.lower(), sql_item=field_item, x=(text_widht/layout_size[0]))))
        self.set_not_to_refresh = True
        self.pressed_filed_list_button = ''



    def on_sql_texinput_change(self):
        # print(self.ids.ti_sql.text)
        self.set_not_to_refresh = True
        self.pressed_filed_list_button = ''
        # if self.previous_sobjcet.lower() <> self.project.get_object_from_sql(self.ids.ti_sql.text).lower():
        sql_fields = self.project.get_fields_from_sql(self.ids.ti_sql.text)
        if sql_fields:
            self.selection = {}
            for sql_item in sql_fields:
                if sql_item.lower() in self.source_object_field_list.keys() and sql_item.lower() not in self.selection.keys():
                    # print('found item {} {}'.format(sql_item, self.source_object_field_list[sql_item.lower()]))
                    self.selection[sql_item.lower()] = self.source_object_field_list[sql_item.lower()]
                else:
                    self.selection[sql_item.lower()] = sql_item
        self.refresh_fileds_buttons()
