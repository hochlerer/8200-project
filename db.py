from dataclasses import dataclass
from dataclasses_json import dataclass_json
from typing import Any, Dict, List, Type
import db_api
import shelve
import os

boolean = False


@dataclass_json
@dataclass
class DBField(db_api.DBField):
    pass


@dataclass_json
@dataclass
class SelectionCriteria(db_api.SelectionCriteria):
    pass


@dataclass_json
@dataclass
class DBTable(db_api.DBTable):
    def __init__(self, name: str, fields: List[DBField], key_field_name: str, hash_index :List[int] = None):
        self.name = name
        self.fields = fields
        self.key_field_name = key_field_name
        self.hash_index = hash_index if hash_index else [False for i in range(len(fields))]

    def count(self) -> int:
        path_file = os.path.join('db_files', self.name + '.db')
        file_name = shelve.open(path_file, writeback=True)
        try:
            count = len(file_name[self.name].keys())
        finally:
            file_name.close()
        return count

    def insert_record(self, values: Dict[str, Any]) -> None:
        if values.get(self.key_field_name) is None:  # there is no primary key
            raise ValueError
        path_file = os.path.join('db_files', self.name + '.db')
        file_name = shelve.open(path_file, writeback=True)
        try:
            if file_name[self.name].get(values[self.key_field_name]):  # record already exists
                raise ValueError
            file_name[self.name][values[self.key_field_name]] = {}
            for dbfield in self.fields:
                field = dbfield.name
                if field == self.key_field_name:
                    continue
                file_name[self.name][values[self.key_field_name]][field] = values[field] if values.get(field) else None
                values.pop(field)
            if 1 < len(values):  # insert unnecessary fields
                self.delete_record(values[self.key_field_name])
                raise ValueError
            for i in range(len(self.hash_index)):  # update hash_index
                if self.hash_index[i]:
                    path_index_file = os.path.join('db_files', f'{self.name}_{self.fields[i].name}_hash_index.db')
                    index_file = shelve.open(path_index_file, writeback=True)
                    if values.get(self.fields[i].name):
                        index_file[values[self.fields[i].name]].append(values[self.key_field_name])
                    index_file.close()
        finally:
            file_name.close()

    def delete_record(self, key: Any) -> None:
        path_file = os.path.join('db_files', self.name + '.db')
        file_name = shelve.open(path_file, writeback=True)
        try:
            if file_name[self.name].get(key) is None:  # if this key is'nt exist
                raise ValueError
            for i in range(len(self.hash_index)):  # update hash_index
                if self.hash_index[i]:
                    if file_name[self.name][key][self.fields[i].name]:
                        path_index_file = os.path.join('db_files', f'{self.name}_{self.fields[i].name}_hash_index.db')
                        index_file = shelve.open(path_index_file, writeback=True)
                        index_file[file_name[self.name][key][self.fields[i].name]].remove(key)
                        index_file.close()
            file_name[self.name].pop(key)
        finally:
            file_name.close()

    def delete_records(self, criteria: List[SelectionCriteria]) -> None:
        list_to_delete = self.query_table(criteria)
        for row in list_to_delete:
            key = row[self.key_field_name]
            self.delete_record(key)

    def get_record(self, key: Any) -> Dict[str, Any]:
        path_file = os.path.join('db_files', self.name + '.db')
        file_name = shelve.open(path_file, writeback=True)
        try:
            if file_name[self.name].get(key) is None:  # if this key is'nt exist
                raise ValueError
            row = file_name[self.name][key]
        finally:
            file_name.close()
        row[self.key_field_name] = key
        return row

    def update_record(self, key: Any, values: Dict[str, Any]) -> None:
        path_file = os.path.join('db_files', self.name + '.db')
        file_name = shelve.open(path_file, writeback=True)
        try:
            if file_name[self.name].get(key) is None:  # if this key is'nt exist
                raise ValueError
            updated_row = {}
            if values.get(self.key_field_name):  # cannot update the primary key
                raise ValueError
            for dbfield in self.fields:
                field = dbfield.name
                if field == self.key_field_name:
                    continue
                if values.get(field):
                    if self.hash_index[self.fields.index(dbfield)]:  # update hash_index
                        path_index_file = os.path.join('db_files', f'{self.name}_{field}_hash_index.db')
                        index_file = shelve.open(path_index_file, writeback=True)
                        index_file[file_name[self.name][key][field]].remove(key)
                        if values[field]:
                            if index_file.get(values[field]) is None:
                                index_file[values[field]] = list()
                            index_file[values[field]].append(key)
                        index_file.close()
                    updated_row[field] = values[field]
                    values.pop(field)
                else:
                    updated_row[field] = file_name[self.name][key][field]
            if values:  # insert unnecessary fields
                raise ValueError
            file_name[self.name][key] = updated_row
        finally:
            file_name.close()

    def query_table(self, criteria: List[SelectionCriteria]) -> List[Dict[str, Any]]:
        path_file = os.path.join('db_files', self.name + '.db')
        file_name = shelve.open(path_file, writeback=True)
        try:
            for criterion in criteria:  # If the criterion is on the key
                if criterion.field_name == self.key_field_name and criterion.operator == "=":
                    if file_name[self.name].get(criterion.value):
                        result = file_name[self.name][criterion.value]
                        result[self.key_field_name] = criterion.value
                        return [result]
                    return []

            indexes = []
            for i in range(len(self.hash_index)):  # If the criterion is on a field that has an index
                if self.hash_index[i]:
                    if indexes:
                        break
                    for criterion in criteria:
                        if self.fields[i].name == criterion.field_name and criterion.operator == "=":
                            path_index_file = os.path.join('db_files', f'{self.name}_{self.fields[i].name}_hash_index.db')
                            index_file = shelve.open(path_index_file, writeback=True)
                            if index_file.get(criterion.value):
                                indexes = index_file[criterion.value]
                            index_file.close()
                            if not indexes:
                                return []
                            break
            desired_rows = []
            if indexes:
                for i in indexes:
                    for criterion in criteria:
                        if self.__is_condition_hold(file_name[self.name][i], criterion) is False:
                            break
                    else:
                        result = file_name[self.name][i]
                        result[self.key_field_name] = i
                        desired_rows.append(result)
                return desired_rows

            for row in file_name[self.name]:
                for criterion in criteria:
                    if criterion.field_name == self.key_field_name:  # if the condition is on the primary key
                        if self.__is_condition_hold({criterion.field_name:row}, criterion) is False:
                            break
                    elif file_name[self.name][row].get(criterion.field_name) is None:  # if this field is'nt exist
                        raise ValueError
                    elif self.__is_condition_hold(file_name[self.name][row], criterion) is False:
                        break
                else:
                    result = file_name[self.name][row]
                    result[self.key_field_name] = row
                    desired_rows.append(result)
        finally:
            file_name.close()
        return desired_rows

    def create_index(self, field_to_index: str) -> None:
        if field_to_index == self.key_field_name:  # No need to index the primary key
            return
        field_names = [field.name for field in self.fields]
        index = field_names.index(field_to_index)
        if self.hash_index[index]:  # index on this field already exist
            return
        path_file = os.path.join('db_files', self.name + '.db')
        file_name = shelve.open(path_file, writeback=True)
        path_index_file = os.path.join('db_files', f'{self.name}_{field_to_index}_hash_index.db')
        index_file = shelve.open(path_index_file, writeback=True)
        path_data_file = os.path.join('db_files', 'DataBase.db')
        data_file = shelve.open(path_data_file, writeback=True)
        try:
            for row in file_name[self.name]:  # if the field_to_index isn't exist. just one iteration
                if file_name[self.name][row].get(field_to_index) is None:
                    raise ValueError
                break
            for row in file_name[self.name]:
                value = file_name[self.name][row][field_to_index]
                if value is None:
                    continue
                if index_file.get(value) is None:
                    index_file[value] = list()
                index_file[value].append(row)

            data_file[self.name]["hash_index"][index] = True
            self.hash_index[index] = True
        finally:
            file_name.close()
            index_file.close()

    def __is_condition_hold(self, data: Dict[Any, Any] , criterion: SelectionCriteria) -> bool:
        if data[criterion.field_name] is None:
            return False
        if criterion.operator == "=":
            return data[criterion.field_name] == criterion.value
        if criterion.operator == "!=":
            return data[criterion.field_name] != criterion.value
        if criterion.operator == "<":
            return data[criterion.field_name] < criterion.value
        if criterion.operator == ">":
            return data[criterion.field_name] > criterion.value
        if criterion.operator == "<=":
            return data[criterion.field_name] <= criterion.value
        if criterion.operator == ">=":
            return data[criterion.field_name] >= criterion.value
        return eval(f'{data[criterion.field_name]}{criterion.operator}{criterion.value}')


@dataclass_json
@dataclass
class DataBase(db_api.DataBase):
    db_tables = {}
    # Put here any instance information needed to support the API

    def __init__(self):
        path_file = os.path.join('db_files', 'DataBase.db')
        file_name = shelve.open(path_file, writeback=True)
        for table_name in file_name:
            DataBase.db_tables[table_name] = DBTable(table_name, file_name[table_name]["fields"], file_name[table_name]["key_field_name"])

    def create_table(self, table_name: str,  fields: List[DBField],  key_field_name: str) -> DBTable:
        if DataBase.db_tables.get(table_name):  # if this table name already exist
            raise ValueError
        if key_field_name not in [field.name for field in fields]:
            raise ValueError
        path_file = os.path.join('db_files', table_name + '.db')
        file_name = shelve.open(path_file, writeback=True)
        try:
            file_name[table_name] = {}
        finally:
            file_name.close()
        path_file = os.path.join('db_files', 'DataBase.db')
        file_name = shelve.open(path_file, writeback=True)
        try:
            file_name[table_name]={}
            file_name[table_name]["fields"] = fields
            file_name[table_name]["key_field_name"] = key_field_name
            file_name[table_name]["hash_index"] = [False for i in range(len(fields))]
        finally:
            file_name.close()
        new_table = DBTable(table_name, fields, key_field_name)
        DataBase.db_tables[table_name] = new_table
        return new_table

    def num_tables(self) -> int:
        return len(DataBase.db_tables)

    def get_table(self, table_name: str) -> DBTable:
        if DataBase.db_tables.get(table_name):
            return DataBase.db_tables[table_name]
        raise ValueError

    def delete_table(self, table_name: str) -> None:
        if None == DataBase.db_tables.get(table_name):
            raise ValueError
        path_file = os.path.join('db_files', 'DataBase.db')
        file_name = shelve.open(path_file, writeback=True)
        try:
            DataBase.db_tables.pop(table_name)
            shelve_file = (os.path.join('db_files', table_name + ".db.bak"))
            os.remove(shelve_file)
            shelve_file = (os.path.join('db_files', table_name + ".db.dat"))
            os.remove(shelve_file)
            shelve_file = (os.path.join('db_files', table_name + ".db.dir"))
            os.remove(shelve_file)
            for field in file_name[table_name]["fields"]:
                if file_name[table_name]["hash_index"][file_name[table_name]["fields"].index(field)]:
                    shelve_file = (os.path.join('db_files', f'{table_name}_{field.name}_hash_index.db.bak'))
                    os.remove(shelve_file)
                    shelve_file = (os.path.join('db_files', f'{table_name}_{field.name}_hash_index.db.dat'))
                    os.remove(shelve_file)
                    shelve_file = (os.path.join('db_files', f'{table_name}_{field.name}_hash_index.db.dir'))
                    os.remove(shelve_file)

            file_name.pop(table_name)
        finally:
            file_name.close()

    def get_tables_names(self) -> List[Any]:
        return [table for table in DataBase.db_tables.keys()]

    def query_multiple_tables(
            self,
            tables: List[str],
            fields_and_values_list: List[List[SelectionCriteria]],
            fields_to_join_by: List[str]
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError
