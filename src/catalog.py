import json
import os
import re
import index

all_table = {}
path = ''
all_index = {}

class table_instance():
    def __init__(self,table_name,primary_key = 0):
        self.name = table_name
        self.primary_key = primary_key

    def new_index(self, column):
        self.indices.append(column)
        for c in self.columns:
            if column == c.column_name:
                c.has_index = True
    
    def del_index(self, column):
        # self.indices.pop(column)
        for c in self.columns:
            if column == c.column_name:
                c.has_index = False

    columns = []
    indices = []

def debug_for_create():
    for t in all_table:
        print(all_table[t].name)
        print(all_table[t].primary_key)
        for c in all_table[t].columns:
            c.print()

class column():
    def __init__(self,column_name,is_unique,type = 'char',length = 16):
        self.column_name = column_name
        self.is_unique = is_unique
        self.type = type
        self.length = length
        self.has_index = False
    
    def print(self):
        print( self.column_name ,' ',   self.is_unique ,' ',   self.type , self.length,' ',self.has_index )

def check_type_in_table(table,values):
    cur_table = all_table[table]
    if len(cur_table.columns) != len(values):
        raise Exception("Catalog Module : table '%s' "
                        "has %d columns." % (table,len(cur_table.columns)))
    for index,i in enumerate(cur_table.columns):
        if i.type == 'int':
            value = int(values[index])
        elif i.type == 'float':
            value = float(values[index])
        else:
            value = values[index]
            if len(value) > i.length:
                raise Exception("Catalog Module : table '%s' : column '%s' 's length"
                         " can't be longer than %d." % (table, i.column_name,i.length))

        if i.is_unique:
            index.check_unique(table,index,value)


def create_table(table,statement):
    global all_table
    primary_place = re.search('primary key *\(',statement).end()
    primary_place_end = re.search('\)',statement[primary_place:]).start()
    primary_key = statement[primary_place:][:primary_place_end].strip()
    cur_table = table_instance(table,primary_key)
    lists = statement.split(',')
    columns = []
    for column_statement in lists[0:len(lists)-1]:
        column_statement = column_statement.strip()
        col_lists = column_statement.split(' ')
        is_unique = False
        type = 'char'
        column_name = col_lists[0]

        if re.search('unique',cat_list(col_lists[1:])) or column_name == primary_key:
            is_unique = True

        if re.search('char',cat_list(col_lists[1:])):
            length_start = re.search('\(',cat_list(col_lists[1:])).start()+1
            length_end = re.search('\)', cat_list(col_lists[1:])).start()
            length = int(cat_list(col_lists[1:])[length_start:length_end])
            type = 'char'
        elif re.search('int', cat_list(col_lists[1:])):
            length = 0
            type = 'int'
        elif re.search('float', cat_list(col_lists[1:])):
            length = 0
            type = 'float'
        else:
            raise Exception("Catalog Module : Unsupported type for %s." % column_name)
        columns.append(column(column_name,is_unique,type,length))
    cur_table.columns = columns
    flag = False
    for index,col in enumerate(cur_table.columns):
        if col.column_name == cur_table.primary_key:
            cur_table.primary_key = index
            col.has_index = True
            flag = True
            break
    if flag == False:
        raise Exception("Catalog Module : primary_key '%s' not exists."
                        % cur_table.primary_key)
    all_table[table] = cur_table


def drop_table(table):
    all_table.pop(table)

def drop_index(index):
    i = all_index[index]
    table_name = i['table']
    for t in all_table:
        if table_name == t:
            all_table[t].del_index(i['column'])
    all_index.pop(index)

def create_index(index_name,table_name,column):
    all_index[index_name] = {'table':table_name,'column':column}
    for t in all_table:
        if table_name == t:
            all_table[t].new_index(column)

def exists_table(table):
    if table in all_table.keys():
        raise Exception("[INVALID VALUE] Catalog Module : table already exists.")

def not_exists_table(table):
    if table in all_table.keys():
        return
    raise Exception("[INVALID VALUE] Catalog Module : table not exists.")

def not_exists_index(index):
    if index in all_index.keys():
        return
    raise Exception("[INVALID VALUE] Catalog Module : index not exists.")

def exists_index(index):
    if index in all_index.keys():
        raise Exception("[INVALID VALUE] Catalog Module : index already exists.")


def cat_list(lists):
    statement = ''
    for i in lists:
        statement = statement + i
    return statement

if __name__ == '__main__':
    pass
