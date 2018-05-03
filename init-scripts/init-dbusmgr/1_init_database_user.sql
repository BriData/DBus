
create database dbusmgr;

CREATE USER dbusmgr IDENTIFIED BY 'HxP31vevLw9PoiT/';

GRANT ALL ON dbusmgr.* TO dbusmgr@'%' IDENTIFIED BY 'HxP31vevLw9PoiT/';

flush privileges; 