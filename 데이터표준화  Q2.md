create table Department(
    department_id smallint unsigned not null primary key,
   department_name varchar(31) not null,
   manager_id smallint unsigned
   );  

create table Job(
    job_id varchar(10) not null primary key,
   job_name varchar(31) not null,
   min_salary smallint not null,
   max_salary smallint not null
   );  
 
create table Employee(
    employee_id smallint unsigned not null primary key,
   first_name varchar(31) not null,
   last_name varchar(31) not null,
   email varchar(45) default 'email',
   tel varchar(31) default 'tel',
   hire_date date not null,
   job_id varchar(10),
   salary decimal(10,2) not null,
   commission_pct decimal(2,2) null default 0,
   manager_id smallint unsigned,
   department_id smallint unsigned
   );  

create table Job_history(
    employee_id smallint unsigned not null primary key,
   start_date date not null,
   end_date date not null,
   job_id varchar(10),
   department_id smallint unsigned
   );  

alter table Job_history
add constraint foreign key fk_jobhistory_employee(employee_id) references Employee(employee_id) on delete no action,
add constraint foreign key fk_jobhistory_job(job_id) references Job(job_id) on delete no action,
add constraint foreign key fk_jobhistory_department(department_id) references Department(department_id) on delete no action;  

alter table Employee
add constraint foreign key fk_employee_department(department_id) references Department(department_id) on delete no action,
add constraint foreign key fk_employee_job(job_id) references Job(job_id) on delete no action,
add constraint foreign key fk_employee_manager(manager_id) references Employee(employee_id) on delete no action;  

alter table Department
add constraint foreign key fk_department_manager(manager_id) references Employee(manager_id) on delete no action;  

alter table Employee
add unique index uk_employee_email(email ASC);
