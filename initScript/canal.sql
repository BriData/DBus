CREATE USER canal IDENTIFIED BY '你的密码:Canal';  

GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'canal'@'%';
GRANT select on schemaName1.tableName1   			  TO canal; 

FLUSH PRIVILEGES; 
