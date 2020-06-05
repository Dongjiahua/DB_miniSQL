# -*- coding:UTF-8 -*-
import sqlparse
from moz_sql_parser import *

class Interpreter():
    def check_syntax(self,parse):
        for token in parse.tokens:
            if(token.match(Token.Error)):
                return True
        return False

    def error(self,error,parse):
        if(error == "SYNTAX ERROR"):
            print("ERROR CODE 01: SYNTAX ERROR in")
        elif (error == "INVALIID NDENTIFIER"):
            print("ERROR CODE 02: INVALIID NDENTIFIER in")
        elif (error == "INVALID VALUE"):
            print("ERROR CODE 03: INVALID VALUE in")
        elif (error == "INVALID ATTR FOR INDEX"):
            print("ERROR CODE 04: INVALID ATTR FOR INDEX in")
        print('\t'+parse+'\n')            

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
        file_parse = self.tokenize(file_name)
        self.debug(file_parse)
        for parse in file_parse:            
            if (self.check_syntax(parse)):  
                self.error("syntax",parse)
                continue
            else:
                pass


    def __init__(self):
        print("Constructing interpreter...")
        self.execute('sql.txt')



    