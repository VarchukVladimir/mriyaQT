__author__ = 'Vladimir'

from kivy.properties import NumericProperty, StringProperty, BooleanProperty, ObjectProperty, DictProperty, ListProperty
from kivy.uix.screenmanager import Screen
from kivy.app import Builder
from kivy.uix.floatlayout import FloatLayout
from kivy.uix.popup import Popup
from os import path as p
import json

recent_projetcs_file = 'recent_projects.ini'

class LoadDialog(FloatLayout):
    load = ObjectProperty(None)
    cancel = ObjectProperty(None)


class SaveDialog(FloatLayout):
    save = ObjectProperty(None)
    text_input = ObjectProperty(None)
    cancel = ObjectProperty(None)


class StartScreen(Screen):
    project = ObjectProperty()
    loadfile = ObjectProperty(None)
    savefile = ObjectProperty(None)
    text_input = ObjectProperty(None)
    # project_list = ListProperty(None)

    # def __init__(self):
    def __init__(self, **kwargs):
        super(Screen, self).__init__(**kwargs)
        if p.exists(recent_projetcs_file):
            with open(recent_projetcs_file) as f:
                recent_projects = f.read().splitlines()
                self.project_files = recent_projects [:10] + list(reversed(recent_projects[10:]))
        else:
            self.project_files = []
        if len(self.project_files) > 0:
            self.ids.st_layout.clear_widgets()
            for project_file in self.project_files:
                # layout_size = self.ids.st_layout.size
                # text_widht = len(field_item) * 9 + 20
                self.ids.st_layout.add_widget(Builder.load_string('''
Button:
    text: '{project_file_name}'
    size_hint_y: None
    height: 30
    on_release: app.go_to_project(self.text)
    '''.format(project_file_name=project_file)))
        # on_release: root.go_to_project(self.text)
        # input_paths = []
        # selection_keys_lower = [p.basename(selection_item) for selection_item in self.selection.keys()]
        # for workflow_item in self.project.project['workflow']:
        #     if workflow_item['title'].lower() in selection_keys_lower:
        #         input_paths.append(workflow_item['output'])
        # print(input_paths)
        # if not refresh_source:
        #     self.ids.ti_input_objects.text = ', '.join(
        #         [selection_values[0] for selection_values in self.selection.values()])

    def dismiss_popup(self):
        self._popup.dismiss()

    def show_load(self):
        content = LoadDialog(load=self.load, cancel=self.dismiss_popup)
        self._popup = Popup(title="Load file", content=content,
                            size_hint=(0.9, 0.9))
        self._popup.open()

    def show_save(self):
        content = SaveDialog(save=self.save, cancel=self.dismiss_popup)
        self._popup = Popup(title="Save file", content=content,
                            size_hint=(0.9, 0.9))
        self._popup.open()

    def load(self, path, filename):
        # self.text_input = str(filename)
        self.ids.text_input.text = str(filename[0])
        print('load project {}'.format(self.ids.text_input.text))
        self.dismiss_popup()

    def save(self, path, filename):
        # self.text_input = filename
        self.ids.text_input.text = p.join(str(path), str(filename))
        print('save project {}'.format(self.ids.text_input.text))
        self.dismiss_popup()
