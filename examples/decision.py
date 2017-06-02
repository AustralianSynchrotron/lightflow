""" Use branching to implement a dynamic decision graph

This workflow demonstrates how to use branching in order to change direction in a path
dynamically. Based on a random number, the graph will either call the small_number_task
or the large_number_task.

"""

from random import random

from lightflow.models import Dag, Action
from lightflow.tasks import PythonTask


# decide which route to go based on a random number
def decide_on_successor(data, store, signal, context):
    data['number'] = random()
    if data['number'] < 0.5:
        return Action(data, limit=[small_number_task])
    else:
        return Action(data, limit=[large_number_task])


# the callback function for the small number route
def print_small_number(data, store, signal, context):
    print('Small number: {}'.format(data['number']))


# the callback function for the large number route
def print_large_number(data, store, signal, context):
    print('Large number: {}'.format(data['number']))


# task definitions
decision_task = PythonTask(name='decision_task',
                           callback=decide_on_successor)

small_number_task = PythonTask(name='small_number_task',
                               callback=print_small_number)

large_number_task = PythonTask(name='large_number_task',
                               callback=print_large_number)


# create the main DAG
d = Dag('main_dag')

d.define({decision_task: [small_number_task, large_number_task]})
