# 个人报告素材-JXR

## 一、Interpreter

### SQL解析器

#### 流程

- 提取SQL单句

  - from 多句字符串 to 单句列表

  - split by 分号

  - in SQL parse

    ```python
    sqlparse.split(sql)
    ```

- 格式化

  - 关键字大小写统一

    ```python
    for stmt in stmts:
        # format格式化
        print(sqlparse.format(stmt, reindent=True, keyword_case="upper")
    ```

- 解析

  - 语句

    ```
    parsed = sqlparse.parse(sql)
    ```

    - return 
      - 实例的元组
      - 所分析语句的树状表示
    - tuple的每一项是split()标识的单个语句
    - tokens属性

  - 基类

  - SQL表示类

#### 自实现

##### BLOG

- 整体思路 <https://blog.csdn.net/qq_36098284/article/details/79912046>
- 单句SQL解析 <https://www.cnblogs.com/pelephone/articles/sql-parse-single-word.html>
- 正则表达式 <https://www.cnblogs.com/darkterror/p/6474211.html>
- interpreter整体思路 <https://blog.csdn.net/zccz14/article/details/51672990>

#### SQL parse

- Github <https://github.com/andialbrecht/sqlparse>
- 文档 <https://sqlparse.readthedocs.io/en/latest/intro/>

#### pyparsing

- SQL查询解析器 <https://github.com/mozilla/moz-sql-parser>
- SQL解析器 <http://jakewheat.github.io/simple-sql-parser/latest/>

### 实际实现

最终发现，想要完成一个完善的SQL parser难度很大，而且调用成熟的库处理难度也很大。miniSQL的要求较为简单，因此，采取自实现的方式。将各语句逐次进行分类，再分别检验、处理和实现。

- 预处理
  - 按分号分割子句
  - 剔除不必要的空格
- 初步分类 - interpreter
  - 根据语句中第一个关键字进行分类
  - 分为create\drop\select\delete\insert\execfile\quit
- 细分类 - API
  - create
    - table
    - index
  - drop
    - table
    - index
- 对各语句分别进行处理 - API

### 测试样例

> create table student2(
> ​	id int,
> ​	name char(12) unique,
> ​	score float,
> ​	primary key(id) 
> );
>
> drop table student;
>
> create index stunameidx on student ( sname );
>
> drop index stunameidx;
>
> select * from student;
> select * from student where sno = ‘88888888’;
> select * from student where sage > 20 and sgender = ‘F’;
>
> insert into student values (‘12345678’,’wy’,22,’M’);
>
> delete from student;
> delete from student where sno = ‘88888888’;
>
>   quit;
>
>
>
> execfile new.txt;
>
>   execfile new.txt;
>
>   。 execfile new.txt;
>
> select * from student whom where sno = ‘88888888’



## 二、API



## 三、Catalog Manager

- 实现 <https://www.write-bug.com/article/2175.html>



### 实际实现

- 表中信息
  - 名称
  - 主键
  - 属性
  - 索引
- 表上字段
  - 属性名
  - 是否唯一
  - 类型
  - 长度
- 全局变量
  - all_table
    - list for table_instance
  - all_index
    - list for index
    - all_index[index_name] = {'table':table_name,'column':column}
- primary key
  - 暂不支持联合
- index
  - 暂不支持联合