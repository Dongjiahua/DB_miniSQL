import json
import os
import re
import index
import api
from api import *
from index_manager import *

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
                if c.is_unique == False:
                    raise Exception("INVALID ATTR FOR INDEX: Please create index on unique attribute.")
                c.has_index = True
    
    def del_index(self, column):
        # self.indices.pop(column)
        for c in self.columns:
            if column == c.column_name:
                c.has_index = False

    def get_column_index(self, column):
        for i in range(0,len(self.columns)):
            if column == self.columns[i].column_name:
                return i
        raise Exception(column + " is an invalid attribute.")
    columns = []
    indices = []
    Tree = {}


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
    value = []
    if len(cur_table.columns) != len(values):
        raise Exception("Catalog Module : table '%s' "
                        "has %d columns." % (table,len(cur_table.columns)))
    for index,i in enumerate(cur_table.columns):
        if i.type == 'int':
            values[index] = int(values[index])
        elif i.type == 'float':
            values[index] = float(values[index])
        elif i.type == 'bool':
            values[index] = bool(values[index])
        elif i.type == 'varchar':
            values[index] = values[index].replace('\'','')
            if len(values[index]) > 255:
                raise Exception("Catalog Module : table '%s' : column '%s' 's length"
                         " can't be longer than %d." % (table, i.column_name,i.length))
        else:
            values[index] = values[index].replace('\'','')
            if len(values[index]) > i.length:
                raise Exception("Catalog Module : table '%s' : column '%s' 's length"
                         " can't be longer than %d." % (table, i.column_name,i.length))
    return values


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
        column_name = col_lists[0].strip()

        if re.search('unique',cat_list(col_lists[1:])) or column_name == primary_key:
            is_unique = True

        if re.search('varchar',cat_list(col_lists[1:])):
            length = 255
            type = 'varchar'
        elif re.search('char',cat_list(col_lists[1:])):
            length_start = re.search('\(',cat_list(col_lists[1:])).start()+1
            length_end = re.search('\)', cat_list(col_lists[1:])).start()
            length = int(cat_list(col_lists[1:])[length_start:length_end])
            type = 'char'
        elif re.search('int', cat_list(col_lists[1:])):
            length = 4
            type = 'int'
        elif re.search('float', cat_list(col_lists[1:])):
            length = 8
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
            if (col.type == 'int'):
                n = calculate_n(4)
            elif(col.type == 'float'):
                n = calculate_n(8)
            else:
                n = calculate_n(col.length)
            tree = Index(n, None, col.column_name)
            cur_table.Tree[index] = tree
            break

    if flag == False:
        raise Exception("Catalog Module : primary_key '%s' not exists."
                        % cur_table.primary_key)
    all_table[table] = cur_table

def convert_result(table,results):
    cur_table = all_table[table]
    for result in results:
        for index,i in enumerate(cur_table.columns):
            if i.type == 'int':
                result[index] = int(result[index])
            elif i.type == 'float':
                result[index] = float(result[index])
            elif i.type == 'bool':
                result[index] = bool(result[index])
    return results


def convert_type(table,condition):
    cur_table = all_table[table]
    i = cur_table.columns[condition['column_id']]
    if i.type == 'int':
        condition['value'] = int(condition['value'])
    elif i.type == 'float':
        condition['value'] = float(condition['value'])
    elif i.type == 'bool':
        condition['value'] = bool(condition['value'])
    else:
        condition['value'] = condition['value'].replace('\'','')
    return condition

def table_print(table,results):
    print("**********************************************************************************************")
    print('\tTABLE\t'+table.name)
    print("**********************************************************************************************")
    for index, i in enumerate(table.columns):
        print('\t'+i.column_name+'\t'+'|',end='')
    print('')
    print("----------------------------------------------------------------------------------------------")

    for i in results:
        for j in i:
            print(j,'\t'+'|'+'\t', end = '')
        print('')
    print("----------------------------------------------------------------------------------------------")


def check_types_of_table(table,values): 
    cur_table = all_table[table]
    if len(cur_table.columns) != len(values):
        raise Exception("Catalog Module : table '%s' "
                        "has %d columns." % (table,len(cur_table.columns)))
    for index,i in enumerate(cur_table.columns):
        if i.type == 'int':
            value = int(values[index])
        elif i.type == 'float':
            value = float(values[index])
        elif i.type == 'bool':
            value = bool(values[index])
        elif i.type == 'varchar':
            value = values[index]
            if len(value) > 255:
                raise Exception("Catalog Module : table '%s' : column '%s' 's length"
                         " can't be longer than %d." % (table, i.column_name,i.length))
        else:
            value = values[index]
            if len(value) > i.length:
                raise Exception("Catalog Module : table '%s' : column '%s' 's length"
                         " can't be longer than %d." % (table, i.column_name,i.length))

        #if i.is_unique:
            #index.check_unique(table,index,value)
def sql_format(args):
    if(re.search('<>', args)):
        return args.replace('<>',' <> ')
    elif(re.search('<=', args)):
        return args.replace('<=',' <= ')
    elif (re.search('>=', args)):
        return args.replace('>=',' >= ')
    elif (re.search('>', args)):
        return args.replace('>',' > ')
    elif (re.search('<', args)):
        return args.replace('<',' < ')
    elif (re.search('=', args)):
        return args.replace('=',' = ')

def check_delete_statement(table,conditions,__columns):
    # raise an exception if something is wrong
    columns = []
    for i in all_table[table].columns:
        columns.append(i.column_name)
    if conditions != '':
        conditions = re.sub('and',',',conditions)
        conditions_lists = conditions.split(',')
        for i in conditions_lists:
            i = sql_format(i)
            if i.strip().split(' ')[0] not in columns:
                raise Exception("Catalog Module : no such column"
                                " name '%s'." % i.strip().split(' ')[0])

def check_select_statement(table,conditions,__columns):
    # raise an exception if something is wrong
    columns = []
    for i in all_table[table].columns:
        columns.append(i.column_name)
    if conditions != '':
        conditions = re.sub('and',',',conditions)
        conditions_lists = conditions.split(',')
        for i in conditions_lists:
            i = sql_format(i)
            if i.strip().split(' ')[0] not in columns:
                raise Exception("Catalog Module : no such column"
                                " name '%s'." % i.strip().split(' ')[0])
    if __columns == '*' or  __columns == '':
        return

    __columns_list = __columns.split(',')
    for i in __columns_list:
        if i.strip() not in columns:
            raise Exception("Catalog Module : no such column name '%s'." % i.strip())


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
            try:
                all_table[t].new_index(column)
            except Exception as e:
                print(str(e))

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
