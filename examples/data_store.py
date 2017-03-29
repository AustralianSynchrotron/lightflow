from lightflow.models import Dag, Action
from lightflow.tasks import PythonTask

import numpy as np


def first_call(name, data, store, signal):
    store.set('number', 5)
    store.set('buffer.observable', 20)
    store.push('sample.spectra', 7)
    store.set('image', np.ones((10, 10)))
    store.set('image', np.ones((100, 100)))

    return Action(data)


def second_call(name, data, store, signal):
    number = store.get('number')
    img = store.get('image')
    print(img.shape)

    store.set('number', number*10)
    store.push('filenames', 'file_a.spec')


def third_a_call(name, data, store, signal):
    store.push('filenames', 'file_b.spec')


def third_b_call(name, data, store, signal):
    store.push('filenames', ['nested_a', 'nested_b'])
    store.extend('filenames', ['file_c.spec', 'file_d.spec'])


d = Dag('myDag')

first = PythonTask(name='first',
                   callable=first_call)

second = PythonTask(name='second',
                    callable=second_call)

third_a = PythonTask(name='third_a',
                     callable=third_a_call)

third_b = PythonTask(name='third_b',
                     callable=third_b_call)

d.define({first: [second], second: [third_a, third_b]})
