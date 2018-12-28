import pro_mig_util as mu

conn_oracle_hr = mu.get_oracle_conn("hr","hrpw", "localhost:1521/xe")
conn_mysql_betterdb = mu.get_mysql_conn("betterdb")
conn_creat_tables = mu.get_mysql_conn("betterdb")


with conn_creat_tables:
    cur = conn_creat_tables.cursor()

    cur.execute("call sp_drop_fk_refs('Job')")
    cur.execute("drop table if exists Job")
    sql_create_job = '''create table Job(job_id varchar(10) not null primary key,
                                         job_name varchar(31) not null,
                                         min_salary decimal(10,2) not null,
                                         max_salary decimal(10,2) not null
                    )'''
    cur.execute(sql_create_job)

    cur.execute("call sp_drop_fk_refs('Job_history')")
    cur.execute("drop table if exists Job_history")
    sql_create_job_history = '''create table Job_history(employee_id smallint unsigned not null,
                                                         start_date date not null,
                                                         end_date date not null,
                                                         job_id varchar(10),
                                                         department_id smallint unsigned
                            )'''
    cur.execute(sql_create_job_history)

    cur.execute("call sp_drop_fk_refs('Department')")
    cur.execute("drop table if exists Department")
    sql_create_department = '''create table Department(department_id smallint unsigned not null primary key, 
                                                       department_name varchar(31) not null, 
                                                       manager_id smallint unsigned,
                                                       employee_cnt smallint unsigned
                            )'''
    cur.execute(sql_create_department)



    cur.execute("call sp_drop_fk_refs('Employee')")
    cur.execute("drop table if exists Employee")
    sql_create_employee = '''create table Employee(employee_id smallint unsigned not null primary key,
                                                   first_name varchar(31) not null,
                                                   last_name varchar(31) not null,
                                                   email varchar(45) default 'email',
                                                   tel varchar(31) default 'tel',
                                                   hire_date date not null,
                                                   job_id varchar(10),
                                                   salary decimal(8,2) not null,
                                                   commission_pct decimal(2,2) default 0,
                                                   manager_id smallint unsigned default 0,
                                                   department_id smallint unsigned
                         )'''
    cur.execute(sql_create_employee)


with conn_oracle_hr:

    cur = conn_oracle_hr.cursor()

    sql_departments = '''select department_id, department_name, manager_id, emp_cnt from departments'''
    cur.execute(sql_departments)
    rows_departments = cur.fetchall()

    sql_employees = '''select employee_id, first_name, last_name, email, phone_number, hire_date, job_id, salary, commission_pct, manager_id, department_id from employees'''
    cur.execute(sql_employees)
    rows_employees = cur.fetchall()

    sql_jobs = '''select job_id, job_title, min_salary, max_salary from jobs'''
    cur.execute(sql_jobs)
    rows_jobs = cur.fetchall()

    sql_job_history = '''select employee_id, start_date, end_date, job_id, department_id from job_history'''
    cur.execute(sql_job_history)
    rows_job_history = cur.fetchall()

for row in rows_departments:
    print("departments >> ",row)
for row in rows_employees:
    print("employees >> ",row)
for row in rows_jobs:
    print("jobs >> ",row)
for row in rows_job_history:
    print("job_history >> ",row)


with conn_mysql_betterdb:
    cur = conn_mysql_betterdb.cursor()

    sql_insert_department = '''insert into Department(department_id, department_name, manager_id, employee_cnt) values(%s, %s, %s, %s)'''
    cur.executemany(sql_insert_department, rows_departments)

    sql_insert_employee = '''insert into Employee(employee_id, first_name, last_name, email, tel, hire_date, job_id, salary, commission_pct, manager_id, department_id) 
                                           values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                          '''
    cur.executemany(sql_insert_employee, rows_employees)

    sql_insert_job = '''insert into Job(job_id, job_name, min_salary, max_salary) 
                                 values(%s, %s, %s, %s)
                     '''
    cur.executemany(sql_insert_job, rows_jobs)

    sql_insert_job_history = '''insert into Job_history(employee_id, start_date, end_date, job_id, department_id) values(%s, %s, %s, %s, %s)'''
    cur.executemany(sql_insert_job_history, rows_job_history)

    cur.execute('''alter table Employee
                     add unique index uk_employee_email(email ASC)''')

    cur.execute('''alter table Job_history 
                     add constraint foreign key fk_jobhistory_employee(employee_id)
                     references Employee(employee_id) on delete no action''')

    cur.execute('''alter table Job_history 
                     add constraint foreign key fk_jobhistory_job(job_id)
                     references Job(job_id) on delete no action''')

    cur.execute('''alter table Job_history 
                     add constraint foreign key fk_jobhistory_department(department_id)
                     references Department(department_id) on delete no action''')

    cur.execute('''alter table Employee
                     add constraint foreign key fk_employee_department(department_id)
                     references Department(department_id) on delete no action''')
     
    cur.execute('''alter table Employee
                     add constraint foreign key fk_employee_job(job_id)
                     references Job(job_id) on delete no action''')
     
    cur.execute('''alter table Employee
                     add constraint foreign key fk_employee_manager(manager_id)
                     references Employee(employee_id) on delete no action''')
     
    cur.execute('''alter table Department
                     add constraint foreign key fk_department_manager(manager_id)
                     references Employee(employee_id) on delete no action''')

    conn_mysql_betterdb.commit()