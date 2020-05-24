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



## 二、API



## 三、Catalog Manager

