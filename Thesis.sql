
CREATE database Thesis;
show tables;
Show databases;

-- DROP database Thesis;

use Thesis;
CREATE TABLE table_countries(
	id INT NOT NULL PRIMARY KEY auto_increment,
	Country varchar(1000) NOT NULL,
    Name varchar(500) NOT NULL,
    Sector varchar(500) NOT NULL
);
	
-- DROP TABLE table_countries;

SELECT * FROM Thesis.table_countries;



use Thesis;
CREATE TABLE table_projects(
	id INT NOT NULL PRIMARY KEY auto_increment,
	Name varchar(1000) NOT NULL,
    date varchar(20) NOT NULL, 
    Amount int not null
);

-- Drop table table_projects;	
use Thesis;
Select * From table_projects;

-- Drop table table_countries;	
use Thesis;
Select * From table_countries;





	
use Thesis;
Select * From table_projects;

use Thesis;
Select * From table_countries;



-- Select countries with min frequency of projects 
select country from table_countries
GROUP BY country
having (COUNT(name) = 1);



-- Select countries with max frequency of projects
select country from table_countries
GROUP BY country
having (COUNT(name) > 1000);



-- Select projects with > 1 billion 
select table_countries.Name from table_projects 
inner join table_countries on table_projects.name = table_countries.name
where Amount > 1000000000;


-- Select projects with < 10.000 
select table_countries.Name from table_projects 
inner join table_countries on table_projects.name = table_countries.name
where Amount < 10000;





-- use Thesis;
-- Select * From despw_thesis.table_projects where Amount > 1000000000.0;
-- Select * From despw_thesis.table_projects;

-- use Thesis;
-- SELECT Name, Amount from table_projects;
-- use despw_thesis;
-- SELECT Date from table_projects;

-- use Thesis;
-- SELECT Country, Name, Sector from table_countries;






