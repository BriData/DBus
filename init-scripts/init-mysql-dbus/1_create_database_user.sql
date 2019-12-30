--- 1 创建库，库大小由dba制定(可以很小，就2张表）
create database dbus;

--- 2  创建用户，密码由dba制定
CREATE USER dbus IDENTIFIED BY 'your_password';


--- 3 授权dbus用户访问dbus自己的库, 需要授权给dbus程序对应的ip段
GRANT ALL ON dbus.* TO dbus@'%'  IDENTIFIED BY 'your_password';
GRANT ALL ON dbus.* TO dbus@'%'  IDENTIFIED BY 'your_password';


flush privileges;