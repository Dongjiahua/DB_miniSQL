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

