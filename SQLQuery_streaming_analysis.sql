CREATE DATABASE toll_data
go
use toll_data;

create table tolldata(
rowid int not null,
date_time datetime,
vehicle_id int,
vehicle_type varchar(20),
plaza_id int,
constraint rowid_pk primary key(rowid))

insert into tolldata values(1, getdate(), 4, 'car', 12)
insert into tolldata values(2, getdate(), 7, 'car', 15)
insert into tolldata values(3, getdate(), 4, 'car', 11)
insert into tolldata values(4, getdate(), 9, 'car', 14)
insert into tolldata values(5, getdate(), 10, 'car', 13)
insert into tolldata values(6, getdate(), 1, 'car', 15)
insert into tolldata values(7, getdate(), 5, 'car', 18)
insert into tolldata values(8, getdate(), 2, 'car', 12)
insert into tolldata values(9, getdate(), 2, 'car', 11)
insert into tolldata values(10, getdate(), 3, 'car', 16)

select * from tolldata

select *, year(date_time) as year, month(date_time) as month, datepart(hour, date_time) as hour from tolldata

--1) Number of vehicles passed through specific toll plaza
select plaza_id, count(vehicle_id) as no_of_vehicles 
from tolldata
group by plaza_id

--2) Number of vehicles passed through specific toll plaza every hour
select datepart(hour, date_time) as hours, plaza_id, count(vehicle_id) as no_of_vehicles 
from tolldata
group by plaza_id, datepart(hour, date_time)

--3) Number of vehicles passed through specific toll plaza every day
select datepart(day, date_time) as days, plaza_id, count(vehicle_id) as no_of_vehicles 
from tolldata
group by plaza_id, datepart(day, date_time)

--4) Number of vehicles passed through specific toll plaza every month
select datepart(month, date_time) as days, plaza_id, count(vehicle_id) as no_of_vehicles 
from tolldata
group by plaza_id, datepart(month, date_time)

--5) Number of vehicles passed through specific toll plaza every year
select datepart(year, date_time) as days, plaza_id, count(vehicle_id) as no_of_vehicles 
from tolldata
group by plaza_id, datepart(year, date_time)





