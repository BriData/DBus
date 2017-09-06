CREATE USER canal IDENTIFIED BY '你的密码:Canal';  

GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'canal'@'%';

GRANT select on schemaName.tableName   			  TO canal; 
GRANT select on dbus.db_heartbeat_monitor         TO canal;
GRANT select on dbus.db_full_pull_requests        TO canal;

FLUSH PRIVILEGES; 
