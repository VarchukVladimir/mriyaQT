__author__ = 'Vladimir Varchuk'
__email__ = 'vladimir.varchuk@rackspace.com'

from kivy.uix.screenmanager import Screen
from kivy.properties import StringProperty
from os import mkdir, getcwd
from os.path import exists


class ReviewScreen(Screen):
    file_name = StringProperty()
    previous_screen = StringProperty()

    def __init__(self, **kwargs):
        super(ReviewScreen, self).__init__(**kwargs)
        self.load_data()

    def load_data(self):
        print(self.file_name)
        print(self.ids.ti_lines.text.isdigit())
        text = ''
        if exists(self.file_name) and self.ids.ti_lines.text.isdigit():
            f = open(self.file_name)
            for i in range(0, int(self.ids.ti_lines.text)):
                text = text + f.readline()
        self.ids.ti_text.text = text
