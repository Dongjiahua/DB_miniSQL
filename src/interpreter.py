# -*- coding:UTF-8 -*-
import sqlparse
import api
import re
import time
from api import *

class Interpreter():
    def check_syntax(self,parse):
        # for token in parse.tokens:
        #     pass
        # return False
        pass

    def error(self,error,parse):
        # if(error == "SYNTAX ERROR"):
        #     print("ERROR CODE 01: SYNTAX ERROR in")
        # elif (error == "INVALIID NDENTIFIER"):
        #     print("ERROR CODE 02: INVALIID NDENTIFIER in")
        # elif (error == "INVALID VALUE"):
        #     print("ERROR CODE 03: INVALID VALUE in")
        # elif (error == "INVALID ATTR FOR INDEX"):
        #     print("ERROR CODE 04: INVALID ATTR FOR INDEX in")
        # print('\t'+parse+'\n')            
        pass

    def debug(self,file_parse):
        for parse in file_parse:            
            for token in parse.tokens:
                print(type(token), " ",token.ttype, " ",token.value)
            print('\n')

    def tokenize(self,file_name):
        with open("test\\"+file_name, 'r', encoding='utf8') as sql_file:
            file_parse = sqlparse.parse(sql_file.read().strip())        
        return file_parse
    
    def execute(self,file_name):
        try:
            f =  open("test\\"+file_name, 'r', encoding='utf8')
        except Exception as e:
            print(str(e))
        text = f.read()
        f.close()
        commands = text.split(';')
        commands = [i.strip().replace('\n','') for i in commands]
        for command in commands:
            if command == '':
                continue
            if command[0] == '#':
                continue
            if command.split(' ')[0] == 'insert':
                try:
                    api.insert(command[6:])
                except Exception as e:
                    print(str(e))
            elif command.split(' ')[0] == 'select':
                try:
                    api.select(command[6:])
                except Exception as e:
                    print(str(e))
            elif command.split(' ')[0] == 'delete':
                try:
                    api.delete(command[6:])
                except Exception as e:
                    print(str(e))
            elif command.split(' ')[0] == 'drop':
                try:
                    api.drop(command[4:])
                except Exception as e:
                    print(str(e))
            elif command.split(' ')[0] == 'create':
                try:
                    api.create(command[6:])
                except Exception as e:
                    print(str(e))
            elif command.split(' ')[0] == 'execfile':
                try:
                    self.execute(command[9:])
                except Exception as e:
                    print(str(e))
            elif command.split(' ')[0] == 'quit':
                try:
                    print("Bye!")
                    return 0
                except Exception as e:
                    print(str(e))
            else :
                print("Interpreter Module : [SYNTAX ERROR] Unrecognized command.")


    def __init__(self):
        print("Constructing interpreter...")
        self.execute('sql.txt')


def select(args):
    time_start = time.time()
    args = re.sub(r' +', ' ', args).strip().replace('\u200b','')
    lists = args.split(' ')
    start_from = re.search('from', args).start()
    end_from = re.search('from', args).end()
    columns = args[0:start_from].strip()
    if re.search('where', args):
        start_where = re.search('where', args).start()
        end_where = re.search('where', args).end()
        table = args[end_from+1:start_where].strip()
        conditions = args[end_where+1:].strip()
    else:
        table = args[end_from+1:].strip()
        conditions = ''
    # CatalogManager.catalog.not_exists_table(table)
    # CatalogManager.catalog.check_select_statement(table,conditions,columns)
    # IndexManager.index.select_from_table(table,conditions,columns)
    time_end = time.time()
    print(" time elapsed : %fs." % (time_end-time_start))


def create(args):
    time_start = time.time()
    args = re.sub(r' +', ' ', args).strip().replace('\u200b','')
    lists = args.split(' ')
    if lists[0] == 'table':
        start_on = re.search('table', args).end()
        start = re.search('\(', args).start()
        end = find_last(args,')')
        table = args[start_on:start].strip()
        statement = args[start + 1:end].strip()
        # CatalogManager.catalog.exists_table(table)
        # IndexManager.index.create_table(table,statement)
        # CatalogManager.catalog.create_table(table,statement)
    elif lists[0] == 'index':
        index_name = lists[1]
        if lists[2] != 'on':
            raise Exception("API Module : Unrecognized symbol for command 'create index',it should be 'on'.")
        start_on = re.search('on',args).start()
        start = re.search('\(',args).start()
        end = find_last(args, ')')
        table = args[start_on:start].strip()
        table = table[3:]
        column = args[start+1:end].strip()
        # CatalogManager.catalog.exists_index(index_name)
        # CatalogManager.catalog.create_index(index_name,table,column)
        # IndexManager.index.create_index(index_name,table,column)
    else:
        raise Exception("API Module : Unrecognized symbol for command 'create',it should be 'table' or 'index'.")
    time_end = time.time()
    if lists[0] == 'table' :
        print("Successfully create table '%s', time elapsed : %fs."
          % (table,time_end - time_start))
    elif lists[0] == 'index':
        print("Successfully create an index of '%s' on table '%s', time elapsed : %fs."
          % (column,table,time_end - time_start))

def drop(args):
    time_start = time.time()
    args = re.sub(r' +', ' ', args).strip().replace('\u200b','')
    if args[0:5] == 'table':
        table = args[6:].strip()
        # CatalogManager.catalog.not_exists_table(table)
        # CatalogManager.catalog.drop_table(table)
        # IndexManager.index.delete_table(table)
        time_end = time.time()
        print("Successfully delete table '%s', time elapsed : %fs." % (table,time_end - time_start))

    elif args[0:5] == 'index':
        index = args[6:].strip()
        # CatalogManager.catalog.not_exists_index(index)
        # CatalogManager.catalog.drop_index(index)
        time_end = time.time()
        print("Successfully delete index '%s', time elapsed : %fs." % (index,time_end - time_start))

    else:
        raise Exception("API Module : Unrecognized symbol for command 'drop',it should be 'table' or 'index'.")


def insert(args):
    time_start = time.time()
    args = re.sub(r' +', ' ', args).strip().replace('\u200b','')
    lists = args.split(' ')
    if lists[0] != 'into':
        raise Exception("API Module : Unrecognized symbol for command 'insert',it should be 'into'.")
    table = lists[1]
    if lists[2] != 'values':
        raise Exception("API Module : Unrecognized symbol for command 'insert',it should be 'values'.")
    value = args[re.search('\(',args).start()+1:find_last(args,')')]
    values = value.split(',')
    # CatalogManager.catalog.not_exists_table(table)
    # CatalogManager.catalog.check_types_of_table(table,values)
    # IndexManager.index.insert_into_table(table,values)
    time_end = time.time()
    print(" time elapsed : %fs." % (time_end-time_start))

def delete(args):
    time_start = time.time()
    args = re.sub(r' +', ' ', args).strip().replace('\u200b','')
    lists = args.split(' ')
    if lists[0] != 'from':
        raise Exception("API Module : Unrecognized symbol for command 'delete',it should be 'from'.")
    elif len(lists) <= 1:
        raise Exception("API Module : Parameter missing for command 'delete',it should be like 'delete from A (where B)'.")
    table = lists[1]
    # CatalogManager.catalog.not_exists_table(table)
    # if len(lists) == 2:
    #     IndexManager.index.delete_from_table(table,[])
    # else:
    #     IndexManager.index.delete_from_table(table,lists[3:])
    time_end = time.time()
    print(" time elapsed : %fs." % (time_end-time_start))

def find_last(string,str):
    last_position=-1
    while True:
        position=string.find(str,last_position+1)
        if position==-1:
            return last_position
        last_position=position

    