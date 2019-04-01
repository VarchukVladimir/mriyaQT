__author__ = 'Vladimir Varchuk'

from kivy.properties import NumericProperty, StringProperty, BooleanProperty, ListProperty
from kivy.uix.screenmanager import Screen
from kivy.app import Builder
from os import path as p


class MSSQL_Query(Screen):
    task_index = NumericProperty()
    task_title = StringProperty()
    task_sql = StringProperty()
    task_type = StringProperty()
    task_output = StringProperty()
    task_source = StringProperty()
    task_exec = BooleanProperty()
    sources_list = ListProperty()

    def __init__(self, **kwargs):
        print('load mssql query')
        super(MSSQL_Query, self).__init__(**kwargs)
        self.project = kwargs['project']
        self.previous_source = self.task_source

    def get_task_name_string(self, task_names):
        if self.ids.ce_source.text in self.project.get_sources() and self.ids.ce_object.text in self.project.get_sobjects(
                self.ids.ce_source.text):
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
        pass

    def on_task_title_changed(self, a):
        self.ids.ti_output.text = p.join(self.project.project_data_dir, self.ids.ti_task_name.text + '.csv')

    def on_text_field_filter(self, input_text):
        if self.source_object_field_list:
            print(self.object_fileds)
            # selection = self.object_fileds.adapter.selection
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
'''.format(sql_item_lower=field_item.lower(), sql_item=field_item, x=(text_widht / layout_size[0]))))
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
