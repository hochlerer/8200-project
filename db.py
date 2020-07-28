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
    # def __init__(self, name: str, fields: List[DBField], key_field_name: str):
    #     self.name = name
    #     self.fields = fields
    #     self.key_field_name = key_field_name
    #     self.hash_index = []

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
        finally:
            file_name.close()

    def delete_record(self, key: Any) -> None:
        path_file = os.path.join('db_files', self.name + '.db')
        file_name = shelve.open(path_file, writeback=True)
        try:
            if file_name[self.name].get(key):
                file_name[self.name].pop(key)
            else:
                raise ValueError
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
            desired_rows = []
            for criterion in criteria:
                if criterion.field_name == self.key_field_name and criterion.operator == "=":
                    if file_name[self.name].get(criterion.value):
                        result = file_name[self.name][criterion.value]
                        result[self.key_field_name] = criterion.value
                        return [result]
                    return []

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
        path_file = os.path.join('db_files', self.name + '.db')
        file_name = shelve.open(path_file, writeback=True)
        try:
            for row in file_name[self.name]:  # if the field_to_index isn't exist. just one iteration
                if file_name[self.name][row].get(field_to_index) is None:
                    raise ValueError
                break
            file_name["hash_index"] = {}
            file_name["hash_index"][field_to_index] = {}
            for row in file_name[self.name]:
                value = file_name[self.name][row][field_to_index]
                if value is None:
                    continue
                if file_name["hash_index"][field_to_index].get(value):
                    file_name["hash_index"][field_to_index][value].append(row)
                else:
                    file_name["hash_index"][field_to_index][value] = row

        finally:
            file_name.close()

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

    # add indexes
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
            file_name.pop(table_name)
        finally:
            file_name.close()
        DataBase.db_tables.pop(table_name)
        shelve_file = (os.path.join('db_files', table_name + ".db.bak"))
        os.remove(shelve_file)
        shelve_file = (os.path.join('db_files', table_name + ".db.dat"))
        os.remove(shelve_file)
        shelve_file = (os.path.join('db_files', table_name + ".db.dir"))
        os.remove(shelve_file)

    def get_tables_names(self) -> List[Any]:
        return [table for table in DataBase.db_tables.keys()]

    def query_multiple_tables(
            self,
            tables: List[str],
            fields_and_values_list: List[List[SelectionCriteria]],
            fields_to_join_by: List[str]
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError
