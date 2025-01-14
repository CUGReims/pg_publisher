DROP SCHEMA IF EXISTS schema_foo CASCADE;
DROP SCHEMA IF EXISTS schema_bar CASCADE;
DROP SCHEMA IF EXISTS schema_baz CASCADE;
CREATE SCHEMA schema_foo;
CREATE SCHEMA schema_bar;
CREATE SCHEMA schema_baz;

CREATE TABLE schema_foo.bar (
    id serial primary key,
    name varchar
);

CREATE TABLE schema_bar.foo (
    id serial primary key,
    name varchar
);

CREATE TABLE schema_bar.dep_bar (
    id serial primary key,
    id_foo_bar int references schema_foo.bar(id)
);

CREATE VIEW schema_bar.viewbar as
    select * from schema_foo.bar;

CREATE MATERIALIZED VIEW schema_bar.matviewbar as
    select * from schema_foo.bar;

CREATE TABLE schema_baz.dep_baz_foo (
    id serial primary key,
    id_foo_bar int references schema_foo.bar(id)
);

CREATE USER not_owner WITH PASSWORD 'not_owner';
GRANT USAGE ON SCHEMA schema_foo TO not_owner;
GRANT SELECT ON TABLE schema_foo.bar TO not_owner;
