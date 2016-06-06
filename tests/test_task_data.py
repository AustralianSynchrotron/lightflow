import pytest
from lightflow.models.task_data import MultiTaskData, TaskData


class TestTaskData:
    def test_construction(self):
        data = MultiTaskData(task_name='test')
        assert data.selected_key == 'test'
        assert data.selected_index == 0

    def test_data_access(self):
        # TODO: Make this more complete
        data = MultiTaskData(task_name='test')
        data['value'] = 5
        data['value'] = data['value']*data['value']

    def test_full_copy(self):
        data = MultiTaskData(task_name='test')
        data.preserve = True
        data['test_string'] = 'a test value'
        data['test_list'] = [1, 2, 3, 4, 5]
        data['test_change_list'] = [6, 7, 8, 9, 10]

        data_copy = data.copy()

        assert data_copy != data
        assert data_copy['test_string'] == data['test_string']
        assert data_copy['test_list'] == data['test_list']

        data['add_value'] = 'another value'
        try:
            result = data_copy['another value']
            raise Exception("This Value Shouldn't exist")
        except KeyError:
            pass

        data['test_change_list'][3] = 17
        assert data_copy['test_change_list'] != data['test_change_list']

    def test_active_copy(self):
        data = MultiTaskData(task_name='test')
        data['value'] = 15
        data.add_dataset('test1')
        data.select_by_task_name('test1')
        data['value'] = 12

        data.select_by_task_name('test')
        data_copy_0 = data.copy()
        data.select_by_task_name('test1')
        data_copy_1 = data.copy()

        assert isinstance(data_copy_0, MultiTaskData)
        assert isinstance(data_copy_1, MultiTaskData)

        assert data_copy_0['value'] != data_copy_1['value']
        assert data.dataset_from_task_name('test')['value'] == data_copy_0['value']
        assert data.dataset_from_task_name('test1')['value'] == data_copy_1['value']
