DROP SCHEMA foo CASCADE;
DROP SCHEMA bar CASCADE;
CREATE SCHEMA foo;
CREATE SCHEMA bar;
CREATE TABLE foo.bar (id int UNIQUE, name varchar);
CREATE TABLE bar.foo (id int UNIQUE, name varchar);
CREATE TABLE bar.dep_bar (id int UNIQUE, id_foo_bar int references foo.bar(id));
CREATE VIEW foo.viewbar as select * from bar.foo;
