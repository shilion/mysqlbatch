# MySQL Sharding 批量执行工具

## 主要功能

批量建库、批量建表、批量执行SQL脚本（创建索引、修改表结构）、Sharding使用情况分析

## 使用方法

python ./mysqlbatch.py [options]

首先需要一个YAML文件来指定要操作的数据库配置，比如：https://github.com/chihongze/mysqlbatch/blob/master/server.yaml

然后就可以搞各种飞机了，参数如下:

* --server: 指定MySQL Server 配置，就是上面的那个yaml文件
* --createdb: 用于第一次使用，会自动执行create database来创建数据库
* --execute: 指定要批量执行的SQL，也可以指定一个SQL脚本，脚本必须以.sql结尾，否则不会识别
* --db:指定在哪些数据库上执行,参数用逗号隔开，如果不指定此项，则会在--server所指定的所有MySQL数据库上执行
* --ana: 分析sharding的使用情况

## 示例：

<pre>
创建数据库，并批量执行createtable.sql脚本
python ./mysqlbatch.py --server ./server.yaml --createdb --execute './createtable.sql'
</pre>

<pre>
批量修改表结构
python ./mysqlbatch.py --server ./server.yaml --execute 'alter table user%04d add column age int not null;'
</pre>


<pre>
在指定库上批量插入某条记录
 python ./mysqlbatch.py --server ./server.yaml --db server_0_499 --execute 'insert into user%04d(id, name)values(0, "Jack");'
</pre>
