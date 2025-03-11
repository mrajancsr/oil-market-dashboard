--Create the entire database if not exists
CREATE DATABASE IF NOT EXISTS securities_master

-- Connect to the database
\c securities_master;

-- Create multiple schemas
CREATE SCHEMA IF NOT EXISTS commodity;

-- Call the commodity schema initialization
\i init_db_commodity.sql