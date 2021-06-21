# 服务器

## 介绍

服务器。

## 安装

### 安装 nodejs

> 如果已安装，请跳过本步。

1. nodejs.org 下载 nodejs 安装文件；
1. 按照提示安装 nodejs；
1. 命令行下执行 `node -v` 验证是否安装正确；

### 安装DB

> 如果已安装，请直接初始化DB。

1. 安装 MariaDB；
1. 登录 MariaDB 并创建数据库

```shell
create database yaloc
```

1. 执行下面的 shell 初始化数据库；

```shell
mysql -uroot -p yaloc < yaloc.sql
```

### 安装 loc-server

1. `mkdir project-dir && cd project-dir`，创建并进入项目目录；
1. `git clone http://xxx/webapp/loc-server.git .`，下载代码库到本地；
1. `npm install`, 安装相关的依赖；
1. `npm start`, 启动开发模式；


## 资料

### node6

讨论：<https://news.ycombinator.com/item?id=11574705>

### node支持的 ES 特性

参见：<http://node.green/>

### node性能

参见：<https://benchmarking.nodejs.org/>

### 使用 babel 转码

参见：<https://github.com/rtsao/babel-preset-es2015-node>

注意：这里 `babel-plugin-transform-es2015-modules-commonjs` 依赖 `babel-plugin-transform-strict-mode`。在安装前者时，会自动安装后者。

