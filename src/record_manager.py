
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
            tt=self.buf.getBlockNum(file_name)
        mk=0
        while mk<entries:
            if bytes2int(self.buf.Blockpool[temp_block].content[mk*8+8:mk*8+12])>0:
                mk+=1
            else:
                break
        for i in range(len(table.columns)):
            if table.columns[i].is_unique and table.columns[i].has_index:
                im.insert(values[i],[tt,8 + 8 * mk],table.Tree[i])

        temp_list=list(self.buf.Blockpool[temp_block].content)
        temp_list[0:4] = list(int2bytes(entries + 1))
        temp_list[4:8] = list(int2bytes(end - len(record)))
        temp_list[8 + 8 * mk:16 + 8 * mk] = list(int2bytes(
            end - len(record)) + int2bytes(len(record)))
        temp_list[end - len(record):end] = list(record)
        self.buf.Blockpool[temp_block].content=bytearray(temp_list)
        self.buf.Blockpool[temp_block].dirty=True

    def get_entries(self,block):
        return bytes2int(self.buf.Blockpool[block].content[0:4])

    def get_free_space(self,block):
        entries=self.get_entries(block)
        mk=0
        addr=0
        while(mk<entries):
            if bytes2int(self.buf.Blockpool[block].content[addr*8+8:addr*8+12])>0:
                mk+=1
            addr+=1
        return bytes2int(self.buf.Blockpool[block].content[4:8])-8-8*addr

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

    def update_record(self,i,table,record,offset,target):
        order=i[1]
        size = bytes2int(self.buf.Blockpool[i[0]].content[order * 8 + 12:order * 8 + 16])
        address = bytes2int(self.buf.Blockpool[i[0]].content[order * 8 + 8:order * 8 + 12])
        entries = self.get_entries(i[0])
        end = bytes2int(self.buf.Blockpool[i[0]].content[4:8])
        templist = list(self.buf.Blockpool[i[0]].content)
        b1=self.value2record(record,table)
        for m in range(len(target)):
            t_column_id=target[m]['column_id']
            t_value=target[m]['value']
            if table.columns[t_column_id].has_index:
                node,i=im.find(record[t_column_id],table.Tree[t_column_id])
                node.keys[i]=t_value
            record[t_column_id]=t_value
        b2=self.value2record(record,table)
        move_length=len(b1)-len(b2)
        mt=0
        mt_a=0
        while mt_a<order:
            if bytes2int(self.buf.Blockpool[i[0]].content[mt_a * 8 + 8:mt_a * 8 + 12])>0:
                mt+=1
            mt_a+=1
        mk=mt+1
        m_addr=order+1
        while mk<entries:
            if bytes2int(self.buf.Blockpool[i[0]].content[mk * 8 + 8:mk * 8 + 12])>0:
                mk+=1
                templist[m_addr*8+8:m_addr*8+12]=list(int2bytes(bytes2int(
                    self.buf.Blockpool[i[0]].content[m_addr * 8 + 8:m_addr * 8 + 12]) + move_length))
            m_addr+=1
        templist[end+move_length:]=list(self.buf.Blockpool[i[0]].content[end:address]+b2+self.buf.Blockpool[i[0]].content[address+size:])
        templist[4:8] = list(int2bytes(end + move_length))
        self.buf.Blockpool[i[0]].content=bytearray(templist)
        self.buf.Blockpool[i[0]].dirty=True

    def tuple_update(self,table,condition,target):
        file_name = self.table2file(table.name)
        offset = self.get_head_length(table)
        block_num = self.buf.getBlockNum(file_name)
        number=0
        index_column = None
        for i in range(len(condition)):
            if table.columns[condition[i]["column_id"]].has_index:
                index_column=condition[i]["column_id"]
                con=i
        if condition[0]['op'] is not None and index_column is not None and condition[con]['op']=="=":
            node,i=im.find(condition[con]['value'],table.Tree[index_column])
            if node is None:
                return 0
            op=condition[con]['op']
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
                        self.update_record([temp_block,order],table,record,offset,target)
                        self.buf.Blockpool[temp_block].dirty = True
                        number+=1
                    elif flag == 2:
                        self.update_record([temp_block,order],table,record,offset,target)
                        self.buf.Blockpool[temp_block].dirty = True
                        number+=1
                        return number
        else:
            for i in range(block_num):
                temp_block = self.buf.loadBlock(file_name, i)
                entries = bytes2int(self.buf.Blockpool[temp_block].content[:4])
                mk=0
                m_addr=0
                while mk<entries:
                    addr = bytes2int(self.buf.Blockpool[temp_block].content[8 + 8 * m_addr:12 + 8 * m_addr])
                    if addr!=0:
                        mk+=1
                        size = bytes2int(self.buf.Blockpool[temp_block].content[12 + 8 * m_addr:16 + 8 * m_addr])
                        record = self.record2value(self.buf.Blockpool[temp_block].content[addr:addr + size],table ,offset)
                        flag = 1
                        for col in condition:
                            flag=self.get_flag(table,col,record,flag)
                        if flag == 1:
                            self.update_record([temp_block,m_addr],table,record,offset,target)
                            self.buf.Blockpool[temp_block].dirty = True
                            number+=1
                        elif flag == 2:
                            self.update_record([temp_block,m_addr],table,record,offset,target)
                            self.buf.Blockpool[temp_block].dirty = True
                            number+=1
                            return number
                    m_addr+=1
            return number

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
        if condition[0]['op'] is not None and index_column is not None and condition[con]['op']=="=":
            node,i=im.find(condition[con]['value'],table.Tree[index_column])
            if node is None:
                return 
            op=condition[con]['op']
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
            for i in range(block_num):
                temp_block = self.buf.loadBlock(file_name, i)
                entries = bytes2int(self.buf.Blockpool[temp_block].content[:4])
                mk=0
                m_addr=0
                while mk<entries:
                    addr = bytes2int(self.buf.Blockpool[temp_block].content[8 + 8 * m_addr:12 + 8 * m_addr])
                    if addr!=0:
                        mk+=1
                        size = bytes2int(self.buf.Blockpool[temp_block].content[12 + 8 * m_addr:16 + 8 * m_addr])
                        record = self.record2value(self.buf.Blockpool[temp_block].content[addr:addr + size],table ,offset)
                        flag = 1
                        for col in condition:
                            flag=self.get_flag(table,col,record,flag)
                        if flag == 1:
                            self.delete_record([temp_block,m_addr],table,record,offset)
                            self.buf.Blockpool[temp_block].dirty = True
                            number+=1
                        elif flag == 2:
                            self.delete_record([temp_block,m_addr],table,record,offset)
                            self.buf.Blockpool[temp_block].dirty = True
                            number+=1
                            return number
                    m_addr+=1
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
        mk=order+1
        while mk<entries:
            templist[mk*8+8,mk*8+12]=list(int2bytes(bytes2int(
                self.buf.Blockpool[i[0]].content[mk * 8 + 8:mk * 8 + 12]) + size))
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
                mk=0
                m_addr=0
                while mk < entries:
                    addr = bytes2int(self.buf.Blockpool[temp_block].content[8 + 8 * m_addr:12 + 8 * m_addr])
                    if addr > 0:
                        mk+=1
                        size = bytes2int(self.buf.Blockpool[temp_block].content[12 + 8 * m_addr:16 + 8 * m_addr])
                        record = self.record2value(self.buf.Blockpool[temp_block].content[addr:addr + size],table, offset)
                        flag=1
                        for col in condition:
                            flag=self.get_flag(table,col,record,flag)
                        if flag==1:
                            result.append(record)
                        elif flag==2:
                            result.append(record)
                            return result
                    m_addr+=1
                    
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
    table=table_instance("student3")
    table.columns=[column("id",True,'char',5),column("age",False,'int')]
    buf=bufferManager()
    RM=record_manager(buf)
    RM.table_create("student3")
    for i in range(1000):
        if i%100==0:
            print(i,i)
        RM.tuple_insert([str(i), i], table)
    result=RM.tuple_select(table,[{"column_id":0,"op":"=","value":"500"}])
    RM.tuple_update(table,[{"column_id":0,"op":"=","value":"500"}],{"column_id":1,"value":7889})
    #RM.tuple_insert(['12347',34],table)
    #RM.tuple_insert(['12346', 34], table)
    #RM.tuple_insert(['12348', 34], table)
    #RM.tuple_delete(table, [{"column_id": 0, "op": "<", "value": "12350"}, {"column_id": 0, "op": ">", "value": "12345"}])
    print(result)
    result=RM.tuple_select(table,[{"column_id":0,"op":"=","value":"500"}])
    print(result)
    RM.buf.writeBackAll()






