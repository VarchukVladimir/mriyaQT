__author__ = 'Vladimir'

from kivy.properties import ObjectProperty
from kivy.uix.screenmanager import Screen
from kivy.app import Builder
from kivy.uix.floatlayout import FloatLayout
from kivy.uix.popup import Popup
from os import path as p
from json import load

recent_projetcs_file = 'recent_projects.ini'
# recent_projetcs_file_sorted = 'recent_projects_sorted.ini'

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
    def __init__(self, **kwargs):
        super(Screen, self).__init__(**kwargs)
        if p.exists(recent_projetcs_file):
            with open(recent_projetcs_file) as f:
                recent_projects = f.read().splitlines()
                self.project_files = recent_projects
        else:
            self.project_files = []
        if len(self.project_files) > 0:
            self.ids.st_layout.clear_widgets()
            for project_file in self.project_files:
                self.ids.st_layout.add_widget(Builder.load_string('''
Button:
    text: '{project_file_name}'
    size_hint_y: None
    height: 30
    on_release: app.go_to_project(self.text)
    '''.format(project_file_name=project_file)))

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
        self.ids.text_input.text = str(filename[0])
        print('load project {}'.format(self.ids.text_input.text))
        self.dismiss_popup()

    def save(self, path, filename):
        self.ids.text_input.text = p.join(str(path), str(filename))
        print('save project {}'.format(self.ids.text_input.text))
        self.dismiss_popup()
