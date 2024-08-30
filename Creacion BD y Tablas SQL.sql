CREATE DATABASE Fase_3_Proyecto;

USE Fase_3_Proyecto;

CREATE TABLE CompanyProfiles ( 
Symbol VARCHAR (50),
Company VARCHAR (50),
Sector VARCHAR (50),
Headquarters VARCHAR (50),
DateFundada VARCHAR (50)
);

CREATE TABLE Companies ( 
Date DATE PRIMARY KEY NOT NULL,
Symbol VARCHAR  NOT NULL , 
Closeprice FLOAT  
);