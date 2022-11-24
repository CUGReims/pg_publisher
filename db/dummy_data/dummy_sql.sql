DROP SCHEMA IF EXISTS schema_foo CASCADE;
DROP SCHEMA IF EXISTS schema_bar CASCADE;
CREATE SCHEMA schema_foo;
CREATE SCHEMA schema_bar;

CREATE TABLE schema_foo.bar (id int UNIQUE, name varchar);
CREATE TABLE schema_bar.foo (id int UNIQUE, name varchar);
CREATE TABLE schema_bar.dep_bar (id int UNIQUE, id_foo_bar int references schema_foo.bar(id));
CREATE VIEW schema_bar.viewbar as select * from schema_foo.bar;
