import re
import time
import catalog
from record_manager import *
from buffer_manager import *
from index_manager import *

global buf 
buf = bufferManager()
global RM
RM=record_manager(buf)

def command_debug(sql,table,conditions,columns):
    print("SQL : ",sql)
    print("Table: ",table)
    print("Conditions: ",conditions)
    print("Columns : ",columns)



def select(args):
    time_start = time.time()
    args = re.sub(r' +', ' ', args).strip().replace('\u200b','')
    lists = args.split(' ')
    start_from = re.search('from', args).start()
    end_from = re.search('from', args).end()
    columns = args[0:start_from].strip()
    conditions = []
    order_flag = False

    if re.search('order by', args):
        start_order = re.search('order by',args).start()
        end_order = re.search('order by', args).end()
        order_flag = True
        order_column = args[end_order+1:].strip()
        args = args[0:start_order].strip()

    if re.search('where', args):
        start_where = re.search('where', args).start()
        end_where = re.search('where', args).end()
        
        table = args[end_from+1:start_where].strip()
        statement = args[end_where+1:].strip()

        catalog.not_exists_table(table)
        catalog.check_select_statement(table,statement,columns)
        statement = statement.split('and')
        
        for i in range(0,len(statement)):
            statement[i] = sql_format(statement[i])
            con_list = statement[i].strip().split(' ')
            while '' in con_list:
                con_list.remove('')
            while ' ' in con_list:
                con_list.remove('')
            try:
                con_list[0] = catalog.all_table[table].get_column_index(con_list[0])
                conditions.append(convert_type(table, {'column_id': con_list[0], 'op':con_list[1], 'value':con_list[2]}))

            except Exception as e:
                print("[INVALID INDENTIFIER] API Module : "+str(e))
            
            results = RM.tuple_select(all_table[table], conditions)

    else:
        table = args[end_from+1:].strip()
        conditions = []
        catalog.not_exists_table(table)
        results = RM.tuple_select(all_table[table], conditions)
        # catalog.check_select_statement(table,conditions,columns)
    #command_debug("select "+args,table,conditions,columns)

    if order_flag == True:
        try:
            order_index = all_table[table].get_column_index(order_column)
            results = sorted(results, key=lambda k: k[order_index]) 
        except Exception as e:
            print("[SYNTAX ERROR] API Module : "+str(e))
    
    time_end = time.time()
    # results = RM.tuple_select(all_table[table], conditions)
    # index.select_from_table(table,conditions,columns)
    # results = convert_result(table,results)
    table_print(all_table[table],results)
    #
    print("%d row(s) affected. Time elapsed : %fs." %(len(results) ,(time_end-time_start)))

def update(args):
    time_start = time.time()
    args = re.sub(r' +', ' ', args).strip().replace('\u200b','')
    try:
        conditions = []
        target = []
        if (re.search('where', args) == None):
            raise Exception("[SYNTAX ERROR] API Module : Parameter missing for command 'update', 'where' is in need.")
        if re.search('set', args) == None:
            raise Exception("[SYNTAX ERROR] API Module : Parameter missing for command 'update', 'set' is in need.")

        start_where = re.search('where', args).start()
        end_where = re.search('where', args).end()
        start_set = re.search('set', args).start()
        end_set = re.search('set', args).end()
        table = args[:start_set].strip()
        statement = args[end_where+1:].strip()

        catalog.not_exists_table(table)
        catalog.check_select_statement(table,statement)
        statement = statement.split('and')
        
        for i in range(0,len(statement)):
            statement[i] = sql_format(statement[i])
            con_list = statement[i].strip().split(' ')
            while '' in con_list:
                con_list.remove('')
            while ' ' in con_list:
                con_list.remove('')
            try:
                con_list[0] = catalog.all_table[table].get_column_index(con_list[0])
                conditions.append(convert_type(table, {'column_id': con_list[0], 'op':con_list[1], 'value':con_list[2]}))

            except Exception as e:
                print("[INVALID INDENTIFIER] API Module : "+str(e))

        statement = args[end_set+1:start_where].strip()            
        catalog.check_select_statement(table,statement)
        statement = statement.split(',')            
    
        for i in range(0,len(statement)):
            statement[i] = sql_format(statement[i])
            con_list = statement[i].strip().split(' ')
            while '' in con_list:
                con_list.remove('')
            while ' ' in con_list:
                con_list.remove('')
            try:
                con_list[0] = catalog.all_table[table].get_column_index(con_list[0])
                target.append(convert_type(table, {'column_id': con_list[0],'value':con_list[2]}))
            except Exception as e:
                print("[INVALID INDENTIFIER] API Module : "+str(e))

        RM.tuple_update(all_table[table],conditions,target)
    except Exception as e:
        print(str(e))
    else:
        time_end = time.time()
        print("Successfully update table '%s', time elapsed : %fs."
            % (table,time_end - time_start))
                
  

def create(args):
    time_start = time.time()
    args = re.sub(r' +', ' ', args).strip().replace('\u200b','')
    lists = args.split(' ')
    if lists[0] == 'table':
        try:
            start_on = re.search('table', args).end()
            start = re.search('\(', args).start()
            end = find_last(args,')')
            table = args[start_on:start].strip()
            statement = args[start + 1:end].strip()
            catalog.exists_table(table)
            # index.create_table(table,statement)
            RM.table_create(table)
            catalog.create_table(table,statement)
        except Exception as e:
            print("API Module : "+str(e))
        else:        
            time_end = time.time()
            print("Successfully create table '%s', time elapsed : %fs."
            % (table,time_end - time_start))
    elif lists[0] == 'index':
        try:
            index_name = lists[1]
            if lists[2] != 'on':
                raise Exception("[SYNTAX ERROR] API Module : Unrecognized symbol for command 'create index',it should be 'on'.")
            start_on = re.search('on',args).start()
            start = re.search('\(',args).start()
            end = find_last(args, ')')
            table = args[start_on:start].strip()
            table = table[3:]
            column = args[start+1:end].strip()
            catalog.exists_index(index_name)
            catalog.create_index(index_name,table,column)
        
            column_id = all_table[table].get_column_index(column)
            c = all_table[table].columns[column_id]

            ctype = c.type 
            if (ctype == 'int'):
                n = calculate_n(4)
            elif(ctype == 'float'):
                n = calculate_n(8)
            else:
                n = calculate_n(c.length)
            
            tree = Index(n, None, column)
            all_table[table].Tree[column_id] = tree
            
            column_id = all_table[table].get_column_index(column)
            RM.index_create(all_table[table],column_id)
        except Exception as e:
            print("API Module : "+str(e))
        else:        
            time_end = time.time()
            print("Successfully create an index of '%s' on table '%s', time elapsed : %fs."
            % (column,table,time_end - time_start))
    else:
        raise Exception("[SYNTAX ERROR] API Module : Unrecognized symbol for command 'create',it should be 'table' or 'index'.")


def drop(args):
    time_start = time.time()
    args = re.sub(r' +', ' ', args).strip().replace('\u200b','')
    if args[0:5] == 'table':
        table = args[6:].strip()
        catalog.not_exists_table(table)
        catalog.drop_table(table)
        # index.delete_table(table)
        time_end = time.time()
        print("Successfully delete table '%s', time elapsed : %fs." % (table,time_end - time_start))

    elif args[0:5] == 'index':
        index = args[6:].strip()
        catalog.not_exists_index(index)
        catalog.drop_index(index)
        # index.delete_table(table)
        time_end = time.time()
        print("Successfully delete index '%s', time elapsed : %fs." % (index,time_end - time_start))

    else:
        raise Exception("[SYNTAX ERROR] API Module : Unrecognized symbol for command 'drop',it should be 'table' or 'index'.")


def insert(args):
    time_start = time.time()
    args = re.sub(r' +', ' ', args).strip().replace('\u200b','')
    lists = args.split(' ')
    if lists[0] != 'into':
        raise Exception("[SYNTAX ERROR] API Module : Unrecognized symbol for command 'insert',it should be 'into'.")
    table = lists[1]
    if 'values' not in lists[2]:
        raise Exception("[SYNTAX ERROR] API Module : Unrecognized symbol for command 'insert',it should be 'values'.")
    value = args[re.search('\(',args).start()+1:find_last(args,')')]
    values = value.split(',')
    catalog.not_exists_table(table)
    values = catalog.check_type_in_table(table,values)

    # index.insert_into_table(table,values)
    try:
        RM.tuple_insert(values,all_table[table])
        time_end = time.time()
        print("Successfully insert %s. Time elapsed : %fs." % (values,(time_end-time_start)))
    except Exception as e:
                print(str(e))

def delete(args):
    time_start = time.time()
    args = re.sub(r' +', ' ', args).strip().replace('\u200b','')
    lists = args.split(' ')
    if lists[0] != 'from':
        raise Exception("[SYNTAX ERROR] API Module : Unrecognized symbol for command 'delete',it should be 'from'.")
    elif len(lists) <= 1:
        raise Exception("[SYNTAX ERROR] API Module : Parameter missing for command 'delete',it should be like 'delete from A (where B)'.")
    table = lists[1]
    catalog.not_exists_table(table)
    # if len(lists) == 2:
    #     record_manager.tuple_delete(table,[])
    start_from = re.search('from', args).start()
    end_from = re.search('from', args).end()
    columns = args[0:start_from].strip()
    conditions = []
    if re.search('where', args):
        start_where = re.search('where', args).start()
        end_where = re.search('where', args).end()

        table = args[end_from+1:start_where].strip()
        statement = args[end_where+1:].strip()

        catalog.not_exists_table(table)
        catalog.check_select_statement(table,statement,columns)
        statement = statement.split('and')
        
        for i in range(0,len(statement)):
            statement[i] = sql_format(statement[i])
            con_list = statement[i].strip().split(' ')
            while '' in con_list:
                con_list.remove('')
            try:
                con_list[0] = catalog.all_table[table].get_column_index(con_list[0])
                conditions.append(convert_type(table, {'column_id': con_list[0], 'op':con_list[1], 'value':con_list[2]}))

            except Exception as e:
                print("[INVALID INDENTIFIER] API Module : "+str(e))

    else:
        table = args[end_from+1:].strip()
        conditions = []
        catalog.not_exists_table(table)
        # catalog.check_delete_statement(table,conditions,columns)

    RM.tuple_delete(all_table[table], conditions)
    time_end = time.time()
    print("Successfully delete. Time elapsed : %fs." % (time_end-time_start))

def write_back():
    buf.writeBackAll()
    #pass

def find_last(string,str):
    last_position=-1
    while True:
        position=string.find(str,last_position+1)
        if position==-1:
            return last_position
        last_position=position