
from api import *
from catalog import *
from buffer_manager import *
import index_manager as im
import math
import os

relation_dic={"<":0,"=":1,">":2}

def compare(a,b):
    if type(a)==int or type(a)==str:
        if a<b:
            return 0
        elif a==b:
            return 1
        else:
            return 2
    if type(a)==float:
        if math.fabs(a-b)<math.fabs(b)/10000.0:
            return 1
        elif a-b>=math.fabs(b)/10000.0:
            return 2
        elif a-b<-math.fabs(b)/10000.0:
            return 0



def intToBin8(i):
    return (bin(((1 << 8) - 1) & i)[2:]).zfill(8)

class record_manager:
    def __init__(self, buffer):
        self.buf = bufferManager()

    def table2file(self, tableName):
        return "TABLE_" + tableName+".txt"

    def table_create(self, tableName):
        filename = self.table2file(tableName)
        tempfile = file()
        tempfile.dirty = True
        self.buf.filePool[filename] = tempfile
        self.buf.writeFile(filename)

    def table_delete(self, tableName):
        file_name=self.table2file(tableName)
        self.buf.deleteFile(file_name)

    def tuple_insert(self, values,table):
        
        condition=[{"column_id":table.primary_key,'op':'=','value':values[table.primary_key]}]
        if len(self.tuple_select(table,condition))>0:
            raise Exception("Error Code: 1062. Duplicate entry for key ")
            return

        file_name=self.table2file(table.name)
        temp_block=None
        record=self.value2record(values,table)

        for i in range(self.buf.getBlockNum(file_name)):
            to_use=self.buf.loadBlock(file_name,i)
            if self.get_free_space(to_use)>8+len(record):
                temp_block=to_use
                tt=i
                break
        if temp_block is not None:
            entries=bytes2int(self.buf.Blockpool[temp_block].content[0:4])
            end=bytes2int(self.buf.Blockpool[temp_block].content[4:8])
        else:
            temp_block=self.buf.getFreeBlock()
            self.buf.filePool[file_name].blocks[self.buf.getBlockNum(file_name)]=temp_block
            self.buf.Blockpool[temp_block].offset=BlockSize*(self.buf.getBlockNum(file_name))
            self.buf.Blockpool[temp_block].fileKey=file_name
            self.buf.filePool[file_name].b_num += 1
            entries=0
            end=BlockSize
            tt=entries+1
        for i in range(len(table.columns)):
            if table.columns[i].is_unique and table.columns[i].has_index:
                im.insert(values[i],[tt,8 + 8 * entries],table.Tree[i])

        temp_list=list(self.buf.Blockpool[temp_block].content)
        temp_list[0:4] = list(int2bytes(entries + 1))
        temp_list[4:8] = list(int2bytes(end - len(record)))
        temp_list[8 + 8 * entries:16 + 8 * entries] = list(int2bytes(
            end - len(record)) + int2bytes(len(record)))
        temp_list[end - len(record):end] = list(record)
        self.buf.Blockpool[temp_block].content=bytearray(temp_list)
        self.buf.Blockpool[temp_block].dirty=True

    def get_entries(self,block):
        return bytes2int(self.buf.Blockpool[block].content[0:4])

    def get_free_space(self,block):
        return bytes2int(self.buf.Blockpool[block].content[4:8])-8-8*self.get_entries(block)

    def record2value(self,record,table,offset):
        addr=0
        values=[]
        bitmap_byte=record[offset:offset+len(table.columns)//8+1]
        bitmap=""
        for i in bitmap_byte:
            bitmap+=intToBin8(i)
        bitmap=bitmap[-len(table.columns):]
        for i in range(len(table.columns)):
            if table.columns[i].type=="int":
                if bitmap[i]=="1":
                    values.append(None)
                else:
                    values.append(bytes2int(record[addr:addr+4]))
                addr+=4
            elif table.columns[i].type=="float":
                if bitmap[i]=="1":
                    values.append(None)
                else:
                    values.append(bytes2float(record[addr:addr+4]))
                addr+=4
            elif table.columns[i].type=="bool":
                if bitmap[i]=="1":
                    values.append(None)
                else:
                    values.append(byte2bool(record[addr:addr+1]))
                addr+=1
            elif table.columns[i].type=="char" or table.columns[i].type=="varchar":
                if bitmap[i]=="1":
                    values.append(None)
                else:
                    loc=bytes2int(record[addr:addr+4])
                    size=bytes2int(record[addr+4:addr+8])
                    values.append(bytes2string(record[loc:loc+size]))
                addr+=8
        return values

    def get_head_length(self,table):
        length=0
        for i in range(len(table.columns)):
            if table.columns[i].type == "int":
                length += 4
            elif table.columns[i].type == "float":
                length += 4
            elif table.columns[i].type == "bool":
                length += 1
            elif table.columns[i].type == 'char' or table.columns[i].type == "varchar":
                length += 8
        return length

    def value2record(self,values,table):
        head=bytearray()
        tail=bytearray()
        offset = len(table.columns)//8+1+self.get_head_length(table)
        bitmap=""
        for i in range(len(table.columns)):
            if values[i] is None:
                bitmap+="1"
                if table.columns[i].type=="int":
                    head+=int2bytes(0)
                elif table.columns[i].type == "float":
                    head+=float2bytes(0.0)
                elif table.columns[i].type=="bool":
                    head+=bool2byte(True)
                elif table.columns[i].type=='char' or table.columns[i].type=="varchar":
                    head+=int2bytes(offset)+int2bytes(0)
            else:
                bitmap+="0"
                if table.columns[i].type=="int":
                    head+=int2bytes(values[i])
                elif table.columns[i].type == "float":
                    head+=float2bytes(values[i])
                elif table.columns[i].type=="bool":
                    head+=bool2byte(values[i])
                elif table.columns[i].type=='char' or table.columns[i].type=="varchar":
                    head+=int2bytes(offset)+int2bytes(len(string2bytes(values[i])))
                    tail+=string2bytes(values[i])
        bitmap="0"*(4-len(table.columns) % 8)+bitmap
        bitmap_byte=bytearray()
        for i in range(0, len(bitmap), 8):
            bitmap_byte += int2byte(int("0b"+bitmap[i:i+8],2))
        return head+bitmap_byte+tail

    def get_flag(self,table,col,record,flag):
        column_id = col['column_id']
        op = col['op']
        value = col['value']
        relation = compare(record[column_id], value)
        if op == "=":
            if relation == relation_dic["="]:
                if table.columns[column_id].is_unique:
                    flag = 2
            else:
                flag = 0
        elif op == '<>' or op == '!=':
            if relation == relation_dic["="]:
                flag = 0
        elif op == '<':
            if relation != relation_dic["<"]:
                flag = 0
        elif op == "<=":
            if relation == relation_dic[">"]:
                flag = 0
        elif op == '>=':
            if relation == relation_dic["<"]:
                flag = 0
        elif op == '>':
            if relation != relation_dic[">"]:
                flag = 0
        elif op is None:
            pass
        return flag

    def tuple_delete(self, table, condition):
        file_name = self.table2file(table.name)
        offset = self.get_head_length(table)
        block_num = self.buf.getBlockNum(file_name)
        number=0
        index_column = None
        for i in range(len(condition)):
            if table.columns[condition[i]["column_id"]].has_index:
                index_column=condition[i]["column_id"]
                con=i
        print("1")
        if condition[0]['op'] is not None and index_column is not None and condition[con]['op']=="=":
            node,i=im.find(condition[con]['value'],table.Tree[index_column])
            if node is None:
                return 
            op=condition[con]['op']
            print("1")
            if op=="=":
                poin = im.recordAll(node, i, condition[con]['value'])
                for ad in poin:
                    order=(ad[1]-8)//8
                    temp_block = self.buf.loadBlock(file_name, ad[0])
                    addr = bytes2int(self.buf.Blockpool[temp_block].content[ad[1]:ad[1] + 4])
                    size = bytes2int(self.buf.Blockpool[temp_block].content[ad[1] + 4:ad[1] + 8])
                    record = self.record2value(self.buf.Blockpool[temp_block].content[addr:addr + size], table, offset)
                    flag = 1
                    for col in condition:
                        flag = self.get_flag(table,col, record, flag)
                    if flag == 1:
                        self.delete_record([temp_block,order],table,record,offset)
                        self.buf.Blockpool[temp_block].dirty = True
                        number+=1
                    elif flag == 2:
                        self.delete_record([temp_block,order],table,record,offset)
                        self.buf.Blockpool[temp_block].dirty = True
                        number+=1
                        return number
        else:
            print("1")
            for i in range(block_num):
                temp_block = self.buf.loadBlock(file_name, i)
                entries = bytes2int(self.buf.Blockpool[temp_block].content[:4])
                k=0
                for j in range(entries):
                    addr = bytes2int(self.buf.Blockpool[temp_block].content[8 + 8 * k:12 + 8 * k])
                    size = bytes2int(self.buf.Blockpool[temp_block].content[12 + 8 * k:16 + 8 * k])
                    record = self.record2value(self.buf.Blockpool[temp_block].content[addr:addr + size],table ,offset)
                    flag = 1
                    for col in condition:
                        flag=self.get_flag(table,col,record,flag)
                    if flag == 1:
                        self.delete_record([temp_block,k],table,record,offset)
                        self.buf.Blockpool[temp_block].dirty = True
                        number+=1
                    elif flag == 2:
                        self.delete_record([temp_block,k],table,record,offset)
                        self.buf.Blockpool[temp_block].dirty = True
                        number+=1
                        return number
                    else:
                        k+=1
            return number

    def delete_record(self,i,table,record,offset):
        order=i[1]
        size = bytes2int(self.buf.Blockpool[i[0]].content[order * 8 + 12:order * 8 + 16])
        address = bytes2int(self.buf.Blockpool[i[0]].content[order * 8 + 8:order * 8 + 12])
        entries = self.get_entries(i[0])
        end = bytes2int(self.buf.Blockpool[i[0]].content[4:8])
        templist = list(self.buf.Blockpool[i[0]].content)
        for t in range(len(table.columns)):
                if table.columns[t].has_index:
                    im.delete(record[t],[self.buf.Blockpool[i[0]].offset//BlockSize,order*8+8],table.Tree[t])
                    node, m=im.find(record[t],table.Tree[t])
        for j in range(order, entries-1):
            templist[j * 8 + 8:j * 8 + 16] = list(self.buf.Blockpool[i[0]].content[j * 8 + 16:j * 8 + 24])
            templist[j * 8 + 8:j * 8 + 12] = list(int2bytes(bytes2int(
                self.buf.Blockpool[i[0]].content[j * 8 + 16:j * 8 + 20]) + size))
            t_addr = bytes2int(self.buf.Blockpool[i[0]].content[j * 8 + 16:j * 8 + 20])
            t_size = bytes2int(self.buf.Blockpool[i[0]].content[j * 8 + 20:j * 8 + 24])
            t_record=self.record2value(self.buf.Blockpool[i[0]].content[t_addr:t_addr + t_size], table, offset)
            for t in range(len(table.columns)):
                if table.columns[t].has_index:
                    node, m=im.find(t_record[t],table.Tree[t])
                    addr = im.recordAll(node, m, t_record[t])
                    if addr is not None:
                        for k in range(len(addr)):
                            if addr[k][0]==i[0] and addr[k][1]==j * 8 + 16:
                                node.pointers[m]=[i[0],j * 8 + 8]
        templist[end + size:] = list(self.buf.Blockpool[i[0]].content[end:address] + \
                                                         self.buf.Blockpool[i[0]].content[address + size:])
        templist[4:8] = list(int2bytes(end + size))
        templist[:4] = list(int2bytes(entries - 1))
        self.buf.Blockpool[i[0]].content=bytearray(templist)
        self.buf.Blockpool[i[0]].dirty=True

    def tuple_select(self, table, condition):
        file_name=self.table2file(table.name)
        offset=self.get_head_length(table)
        result=[]
        index_column=None
        
        for i in range(len(condition)):
            if table.columns[condition[i]["column_id"]].has_index:
                index_column=condition[i]["column_id"]
                con=i
        block_num=self.buf.getBlockNum(file_name)
        
        if condition[0]['op'] is not None and index_column is not None and condition[con]['op']=="=":
       
            node,i=im.find(condition[con]['value'],table.Tree[index_column])
            if node is None:
                return result
            op=condition[con]['op']
   
            if op=="=":
                poin = im.recordAll(node, i, condition[con]['value'])
            for ad in poin:
                temp_block = self.buf.loadBlock(file_name, ad[0])
                addr = bytes2int(self.buf.Blockpool[temp_block].content[ad[1]:ad[1]+4])
                size = bytes2int(self.buf.Blockpool[temp_block].content[ad[1]+4:ad[1]+8])
                record = self.record2value(self.buf.Blockpool[temp_block].content[addr:addr + size],table, offset)
                flag=1
                for col in condition:
                    flag=self.get_flag(table,col,record,flag)
                if flag==1:
                    result.append(record)
                elif flag==2:
                    result.append(record)
                    return result
        else:
            for i in range(block_num):
                temp_block = self.buf.loadBlock(file_name, i)
                entries = bytes2int(self.buf.Blockpool[temp_block].content[:4])
                for j in range(entries):
                    addr = bytes2int(self.buf.Blockpool[temp_block].content[8 + 8 * j:12 + 8 * j])
                    size = bytes2int(self.buf.Blockpool[temp_block].content[12 + 8 * j:16 + 8 * j])
                    record = self.record2value(self.buf.Blockpool[temp_block].content[addr:addr + size],table, offset)
                    flag=1
                    for col in condition:
                        flag=self.get_flag(table,col,record,flag)
                    if flag==1:
                        result.append(record)
                    elif flag==2:
                        result.append(record)
                        return result
        return result

    def index_create(self,table,column_id):
        file_name = self.table2file(table.name)
        offset = self.get_head_length(table)
        block_num = self.buf.getBlockNum(file_name)
        for i in range(block_num):
            temp_block = self.buf.loadBlock(file_name, i)
            entries = bytes2int(self.buf.Blockpool[temp_block].content[:4])
            for j in range(entries):
                addr = bytes2int(self.buf.Blockpool[temp_block].content[8 + 8 * j:12 + 8 * j])
                size = bytes2int(self.buf.Blockpool[temp_block].content[12 + 8 * j:16 + 8 * j])
                record = self.record2value(self.buf.Blockpool[temp_block].content[addr:addr + size], table, offset)
                im.insert(record[column_id],[i,8 + 8 * j],table.Tree[column_id])
        return

if __name__ == '__main__':
    table=table_instance("student")
    table.columns=[column("id",True,'char',5),column("age",False,'int')]
    buf=bufferManager()
    RM=record_manager(buf)
    RM.table_create("student")
    for i in range(1000):
        if i%100==0:
            print(i,i)
        RM.tuple_insert([str(i), 34], table)

    #RM.tuple_insert(['12347',34],table)
    #RM.tuple_insert(['12346', 34], table)
    #RM.tuple_insert(['12348', 34], table)
    #RM.tuple_delete(table, [{"column_id": 0, "op": "<", "value": "12350"}, {"column_id": 0, "op": ">", "value": "12345"}])
    result=RM.tuple_select(table,[{"column_id":0,"op":"=","value":"9900"}])
    RM.buf.writeBackAll()
    print(result)
    RM.buf.writeBackAll()






