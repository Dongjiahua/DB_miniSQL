import sys
import struct
import os


intSize = 16
floatSize = 16

BlockSize = 4000
BlockNum = 10
FilePoolSize = 10

INT = 0
FLOAT = 1
CHAR = 2
BOOL = 3
VARCHAR = 4

FALSE = 0
TRUE = 1


def bytes2float(bs):
    ba = bytearray()
    ba.append(bs[0])
    ba.append(bs[1])
    ba.append(bs[2])
    ba.append(bs[3])
    return struct.unpack("!f", ba)[0]

def float2bytes(f):
    bs = struct.pack("f", f)
    return bs

def string2bytes(s):
    return s.encode(encoding='utf-8')

def bytes2string(b):
    return str(b,'utf-8')


def bytes2int(bytes_data):
    return int.from_bytes(bytes_data, byteorder='big', signed=False)

def int2bytes(int_data):
    return int_data.to_bytes(length=4, byteorder='big', signed=False)


def int2byte(int_data):
    return int_data.to_bytes(length=1, byteorder='big', signed=False)

def bool2byte(b):
    if b:
        return 1
    return 0

def byte2bool(b):
    if b==1:
        return True
    return False


class bufferManager:
    def __init__(self):
        self.filePool = {}

        self.Blockpool = [bufferBlock() for i in range(BlockNum)]
        self._poolState = [True for i in range(BlockNum)]

    def getFreeBlock(self):
        if True in self._poolState:
            return self._poolState.index(True)
        else:
            while True:
                for i in range(BlockNum):
                    if self.Blockpool[i].clock == 0 and not self.Blockpool[i].pin:
                        self.freeBlock(i)
                        return i
                    else:
                        self.Blockpool[i].clock -= 1

    def freeBlock(self, key):
        self.writeBlock(key)
        del self.filePool[self.Blockpool[key].fileKey].blocks[key]
        if len(self.filePool[self.Blockpool[key].fileKey].blocks)==0:
            del self.filePool[self.Blockpool[key].fileKey]
        self.Blockpool[key].clear()
        self._poolState[key] = True

    def deleteFile(self,fileName):
        for i in self.filePool[fileName].blocks.values:
            self.Blockpool[i].clear()
            self._poolState[i]=True
        del self.filePool[fileName]
        os.remove(fileName)

    def loadFile(self, fileName):
        if fileName not in self.filePool.keys():
            blockNeed = os.path.getsize(fileName) // BlockSize
            self.filePool[fileName] = file()
            self.filePool[fileName].b_num=blockNeed
            for i in range(blockNeed):
                self.loadBlock(fileName,i)

    def getBlockNum(self,fileName):
        if fileName not in self.filePool.keys():
            blockNeed = os.path.getsize(fileName) // BlockSize
            self.filePool[fileName] = file()
            self.filePool[fileName].b_num = blockNeed
        return self.filePool[fileName].b_num

    def loadBlock(self,fileName,block):
        if not(fileName in self.filePool.keys() and block in self.filePool[fileName].blocks.keys()):
            if fileName not in self.filePool.keys():
                self.filePool[fileName]=file()
            to_use=self.getFreeBlock()
            self.filePool[fileName].blocks[block]=to_use
            with open(fileName, 'rb') as input:
                input.seek(block*BlockSize,0)
                self.Blockpool[to_use].offset = block * BlockSize
                self.Blockpool[to_use].content=input.read(BlockSize)
                self.Blockpool[to_use].fileKey=fileName
                self._poolState[to_use] = False
            input.close()
        else:
            to_use=self.filePool[fileName].blocks[block]
        return to_use


    def writeBlock(self,block):
        with open(self.Blockpool[block].fileKey, 'rb+') as output:
            if self.Blockpool[block].dirty:
                output.seek(self.Blockpool[block].offset, 0)
                output.write(self.Blockpool[block].content)
                self.Blockpool[block].dirty = False
            output.close()


    def writeFile(self, fileName):
        if len(self.filePool[fileName].blocks)>0:
            for i in self.filePool[fileName].blocks.values():
                self.writeBlock(i)
        else:
            with open(fileName, 'ab+') as output:
                output.close()


    def writeBackAll(self):
        for i in self.filePool.keys():
            self.writeFile(i)

    def setPin(self,blockAddr,choice):
        self.Blockpool[blockAddr].pin=choice


class file:
    def __init__(self):
        self.dirty = False
        self.artributes = {}
        self.index = FALSE
        self.b_num = 0
        self.blocks = {}


class bufferBlock:
    def __init__(self):
        self.offset = 0
        self.fileKey = None
        self.dirty = False
        self.content = bytearray(BlockSize)
        self.clock = 1
        self.pin = False

    def clear(self):
        self.pin = False
        self.offset = 0
        self.fileKey = None
        self.dirty = False
        self.clock = 1
