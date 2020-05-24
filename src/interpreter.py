# -*- coding:UTF-8 -*-
import sqlparse
import api
import re
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
                    pass
                except Exception as e:
                    print(str(e))
            else :
                print("SYNTAX ERROR")


    def __init__(self):
        print("Constructing interpreter...")
        self.execute('sql.txt')



    