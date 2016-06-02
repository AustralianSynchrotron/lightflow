from time import sleep
from lightflow.models import Dag
from lightflow.tasks import PythonTask


def square_value(name, data):
    print(name)
    data['value'] = data['value']*data['value']
    return data


def print_value(name, data):
    print(name)
    print(data['value'])


def put_data_me(name, data):
    print(name)
    data['value'] = 5
    return data


def mult_data(name, data):
    print(name)
    data['value'] = data['value']*data.dataset_from_index(1)['value']
    return data


def sub_data(name, data):
    print(name)
    data['value'] = data.dataset_from_alias('first')['value']-data.dataset_from_alias('second')['value']


d = Dag('myDag')

put_me = PythonTask(name='put_me',
                    python_callable=put_data_me,
                    dag=d)

print_me = PythonTask(name='print_me',
                      python_callable=print_value,
                      dag=d)

square_me = PythonTask(name='square_me',
                       python_callable=square_value,
                       dag=d)

print_me2 = PythonTask(name='print_me2',
                       python_callable=print_value,
                       dag=d)

mult_me = PythonTask(name='mult_me',
                     python_callable=mult_data,
                     dag=d)

print_me3 = PythonTask(name='print_me3',
                       python_callable=print_value,
                       dag=d)

sub_me = PythonTask(name='sub_me',
                    python_callable=sub_data,
                    dag=d)

print_me4 = PythonTask(name='print_me4',
                       python_callable=print_value,
                       dag=d)


d.define_workflow({put_me: {print_me: '', square_me: '', mult_me: '', sub_me: 'second'},
                   square_me: {print_me2: '', mult_me: ''},
                   mult_me: {print_me3: '', sub_me: 'first'},
                   sub_me: [print_me4]})

# d.define_workflow({put_me: [t2, t3], buffer_retrieval: {subtract:subract.BUFFER}, t3: {t4:t4.SAMPLE}})

# print_me.add_downstream(put_me)
# square_me.add_downstream(put_me)
# print_me2.add_downstream(square_me)
# mult_me.add_downstream(square_me)
# mult_me.add_downstream(put_me)
# print_me3.add_downstream(mult_me)


# {t1: [t2, t3], buffer_retrieval: {subtract:subract.BUFFER}, t3: {t4:t4.SAMPLE}}