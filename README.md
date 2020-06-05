# DB_miniSQL
MiniSQL for DB course

## 文件目录说明

- data

  - 预留

- doc

  - 记录文档

- options

  - 废弃代码
  - 别人写的基于pyparse的SQL parser

- src

- test

  - 测试文件
  - 其中sql.txt为固定测试文件

## 接口说明

- 位置 api.py

- 目前是丐版…后续会把catalog中的处理移到这个部分，各位需要什么预处理可以提出来

- select

  - table 表名
  - condition 条件
    - 尚未翻译为内部语言
    - 形如“ A op B”的字符串
  - columns
    - 列名
    - 形如“A,B,C"的字符串
    - 空缺为”*“

- create

  - table

    - name

    - statement

      ```
      id int, name char(12) unique, score float, 	primary key(id) 
      ```

  - index

    - name
    - table
    - column

- drop

  - table name
  - index name

- insert

  - table

  - values

    ```
    ‘12345678’,’wy’,22,’M’
    ```

- delete

  - table
  - lists
    - 以空格分割的where子句


## 待讨论

- 查询结果的输出
  - 格式
  - 查询结果返回的格式

## 已实现

- Interpreter
  - 初步语法检查
- API
  - 初步解析
- catalog
  - create table/index
    - 可以正常存储表和索引的元数据
  - 
- 中文
  - UTF-8

## 待办

- check before select & insert
- select中进一步分割
  - where子句各查询条件
  - 多表的名称
- catalog metadata的文件读写
- 加注释
  - so有疑惑请戳我

## 一些坑

- SQL语句分割直接用的分号
  - 可能会出现非常感人的有意无意的灾难性的注入
- insert语句中values字段的判断用的是in
  - 可能valuesxxxxx也不会报错…

