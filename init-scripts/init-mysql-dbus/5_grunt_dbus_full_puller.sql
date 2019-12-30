---获取拟拉取的目标表备库的读权限，用于初始化加载:
 GRANT select on test_db.test_table1 TO dbus;
 GRANT select on test_db.test_table2 TO dbus;
 GRANT select on test_db1.test1_table1 TO dbus;

flush privileges;