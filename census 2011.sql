select * from project_1..data1
select * from project_1.. sheet1

--number of rows in our dataset
select count(*) from project_1..data1
select count(*) from project_1.. sheet1

-- search district contain avi
select district ,state from  project_1..sheet1
where  district like '%avi%'


-- search district contain  avi word
select district ,state from  project_1..sheet1
where  district like '[avi]%'

select district ,state from  project_1..sheet1
where  district like '%[^vi]%'


--   literacy rate  rank  of districs in each  state
select * ,
dense_rank() over(partition by state order by literacy desc)
from project_1..data1
order by state desc

--  top 5 literate district   for  each  state
with a as
(select * ,
dense_rank() over(partition by state order by literacy desc)rnk
from project_1..data1
) 
select * from a
where rnk <=5
order by  state desc

-- find first , last , and nth rank district in state
select *,
first_value(district) over(partition by state order by literacy  desc) as most_literate,
last_value(district) over(partition by state order by literacy  desc 
range between unbounded preceding and unbounded following) as lest_literate
from project_1..data1

-- total population of indian state in desc order
select state,  sum(population) as totalpopulation from sheet1
group by state
order by totalpopulation desc 

-- total population of india
select sum(population) as total_population_of_india from sheet1 

-- avg growth 
select  state, round(avg(growth)*100,2)  as avggrowth from data1
group by state 
order by avggrowth desc

-- avg sex ratio
select  state, round(avg(sex_ratio),0)  as avg_sex_ratio from data1
group by state 
order by avg_sex_ratio desc


-- avg literacy rate
select  state, round(avg(literacy),0)  as avg_literacy from data1
group by state 
having round(avg(literacy),0)>70
order by avg_literacy desc

--top 3 district in literacy
with a
as
(select  district, round(avg(literacy),0)  as avg_literacy from data1
group by district 
having round(avg(literacy),0)>70)
select top 3 * from a
order by avg_literacy desc

--top 3 least state in literacy
select top 3 state, round(avg(literacy),0) as a from PROJECT_1..data1
group by state
order by a asc

--top 3 least district in literacy
select top 3 district, round(avg(literacy),0) as a from data1
group by district
order by a asc

-- top and bottom 3 state in literacy 
drop table if exists topstates
create table topstates
( state nvarchar (150),
    topstate float	
	)
insert into topstates
select  state, CAST(round(avg(literacy),0) AS INT) as a from project_1..data1
group by state
order by a desc;
select  top 3* from topstates order by topstates.topstate desc

drop table if exists bottomstates
create table bottomstates
( state nvarchar (150),
    bottomstate float
 )
insert into bottomstates
select  state, round(avg(literacy),0)  a from project_1..data1
group by state

select top 3 *  from bottomstates order by bottomstates.bottomstate asc

-- view table
create view census_2011 as
select top 3 state, round(avg(literacy),0) as a from PROJECT_1..data1
group by state
order by a asc

select* from census_2011

drop view census_2011

--finding male and female population and top 5 state in female population wrt male population
select top 5 y.state,sum(y.male) total_male_population, sum(y.female) total_female_population, rank() over(  order by sum(y.female)/(sum(y.female)+sum(y.male)) desc) rk from
(select x.district,x.state, round(x.population/(x.sex_ratio+1),0) male,round(x.population*x.sex_ratio/(x.sex_ratio+1),0)female from
(select a.district, a.state, a.sex_ratio/1000 sex_ratio, b.population from PROJECT_1..data1 a
inner join project_1..sheet1 b  on a.district =b.district)x)y
group by y.state

--finding male and female population and first highest  female population  in each state
select y.district,y.state,(y.male) total_male_population, (y.female) total_female_population, first_value(y.district)  over(partition by y.state  order by y.female desc) rk from
(select x.district,x.state, round(x.population/(x.sex_ratio+1),0) male,round(x.population*x.sex_ratio/(x.sex_ratio+1),0)female from
(select a.district, a.state, a.sex_ratio/1000 sex_ratio, b.population from PROJECT_1..data1 a
inner join project_1..sheet1 b  on a.district =b.district)x)y

--finding male and female population and least female population contain district in every state
select y.district,y.state,(y.male) total_male_population, (y.female) total_female_population,
last_value(y.district)  over(partition by y.state  order by y.female desc range between unbounded preceding and  unbounded following) from
(select x.district,x.state, round(x.population/(x.sex_ratio+1),0) male,round(x.population*x.sex_ratio/(x.sex_ratio+1),0)female from
(select a.district, a.state, a.sex_ratio/1000 sex_ratio, b.population from PROJECT_1..data1 a
inner join project_1..sheet1 b  on a.district =b.district)x)y
order by y.state desc


-- top 5 least literate population

select top 5 y.state, sum(y.literate_population) literate_population_population, sum(y.illiterate_population) illiterate_population_population ,
(sum(y.literate_population)/(sum(y.literate_population)+sum(y.illiterate_population)))*100 pr from
(select x.district,x.state, round(x.population*x.literacy_ratio,0) literate_population,round(x.population*(1-x.literacy_ratio),0) illiterate_population from
(select a.district, a.state, (a.literacy/100) literacy_ratio, b.population from PROJECT_1..data1 a
inner join project_1..sheet1 b  on a.district =b.district)x)y
group by y.state
order by pr