from dataclasses import dataclass
from dataclasses_json import dataclass_json
from typing import Any, Dict, List, Type
import shelve


@dataclass_json
@dataclass
class DBField:
    name: str
    type: Type


@dataclass_json
@dataclass
class SelectionCriteria:
    field_name: str
    operator: str
    value: Any


@dataclass_json
@dataclass
class DBTable:
    name: str
    fields: List[DBField]
    key_field_name: str



@dataclass_json
@dataclass
class DataBase:
    # Put here any instance information needed to support the API
    def create_table(self,
                     table_name: str,
                     fields: List[DBField],
                     key_field_name: str) -> DBTable:

        file_name = shelve.open(f'{table_name}.db', writeback=True)
        try:
            file_name[table_name] = {}
        finally:
            file_name.close()
        return DBTable(table_name, fields, key_field_name)

    def num_tables(self) -> int:
        raise NotImplementedError

    def get_table(self, table_name: str) -> DBTable:
        raise NotImplementedError

    def delete_table(self, table_name: str) -> None:
        raise NotImplementedError

    def get_tables_names(self) -> List[Any]:
        raise NotImplementedError

    def query_multiple_tables(
            self,
            tables: List[str],
            fields_and_values_list: List[List[SelectionCriteria]],
            fields_to_join_by: List[str]
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError
