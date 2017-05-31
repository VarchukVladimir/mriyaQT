__author__ = 'Vladimir'

from kivy.properties import NumericProperty, StringProperty, BooleanProperty, ObjectProperty, DictProperty
from kivy.uix.screenmanager import Screen
from kivy.app import Builder
from kivy.uix.floatlayout import FloatLayout
from kivy.uix.popup import Popup
from os import path as p


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
        print(path)
        print(filename)
        # self.text_input = str(filename)
        self.ids.text_input.text = str(filename[0])
        print('load project {}'.format(self.ids.text_input.text))
        self.dismiss_popup()

    def save(self, path, filename):
        print(path)
        print(str(filename))
        # self.text_input = filename
        self.ids.text_input.text = p.join(str(path), str(filename))
        print('save project {}'.format(self.ids.text_input.text))
        self.dismiss_popup()
