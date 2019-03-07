---说明：dbusmgr是 dbus的管理库，管理元数据，只需要创建一次，位置在任何mysql库都可以


--- 1 创建库，库大小由dba指定 (很小就可以，存配置使用）
create database dbusmgr;


--- 2 创建用户，密码由dba指定
CREATE USER dbusmgr IDENTIFIED BY 'HxP31vevLw9PoiT/';


--- 3 授权用户，授权自己的库, 密码由dba指定
GRANT ALL ON dbusmgr.* TO dbusmgr@'%' IDENTIFIED BY 'HxP31vevLw9PoiT/';

flush privileges; 