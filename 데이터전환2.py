import pro_mig_util as mu

conn_mysql_betterdb = mu.get_mysql_conn('betterdb')
conn_oracle_hr = mu.get_oracle_conn("hr","hrpw", "localhost:1521/xe")
table_department = 'Department'
table_departments = 'Departments'
table_employee = 'Employee'
table_employees = 'Employees'
table_job = 'Job'
table_jobs = 'Jobs'
table_job_history = 'Job_history'
cols_department = "department_id, department_name, manager_id, employee_cnt"
cols_departments = "department_id, department_name, manager_id, emp_cnt"
cols_employee = "employee_id, first_name, last_name, email, tel, hire_date, job_id, salary, commission_pct, manager_id, department_id"
cols_employees = "employee_id, first_name, last_name, email, phone_number, hire_date, job_id, salary, commission_pct, manager_id, department_id"
cols_job = "job_id, job_name, min_salary, max_salary"
cols_jobs = "job_id, job_title, min_salary, max_salary"
cols_job_history = "employee_id, start_date, end_date, job_id, department_id"
cols_job_historys = "employee_id, start_date, end_date, job_id, department_id"
rand_row_count = 0
# read from source db
with conn_oracle_hr:
    oracle_departments_cnt = mu.get_count(conn_oracle_hr, table_departments)
    oracle_employees_cnt = mu.get_count(conn_oracle_hr, table_employees)
    oracle_jobs_cnt = mu.get_count(conn_oracle_hr, table_jobs)
    oracle_job_history_cnt = mu.get_count(conn_oracle_hr, table_job_history)

    cur = conn_oracle_hr.cursor()
    sql = "select * from (select " + cols_departments + " from " + table_departments + " order by dbms_random.random) where rownum <= :1"
    
    
    sample_rowcount_oracle_departments = round(oracle_departments_cnt / 3)
    cur.execute(sql, (sample_rowcount_oracle_departments,))
    departments_samples = cur.fetchall()


    sql = "select * from (select " + cols_employees + " from " + table_employees + " order by dbms_random.random) where rownum <= :1"

    sample_rowcount_oracle_employees = round(oracle_employees_cnt / 3)
    cur.execute(sql,(sample_rowcount_oracle_employees,))
    employees_samples = cur.fetchall()


    sql = "select * from (select " + cols_jobs + " from " + table_jobs + " order by dbms_random.random) where rownum <= :1"
    sample_rowcount_oracle_jobs = round(oracle_jobs_cnt / 3)
    cur.execute(sql,(sample_rowcount_oracle_jobs,))
    jobs_samples = cur.fetchall()


    sql = "select * from (select " + cols_job_historys + " from " + table_job_history + " order by dbms_random.random) where rownum <= :1"
    sample_rowcount_oracle_job_historys = round(oracle_job_history_cnt / 3)
    cur.execute(sql,(sample_rowcount_oracle_job_historys,))
    job_historys_samples = cur.fetchall()



with conn_mysql_betterdb:
    mysql_department_cnt = mu.get_count(conn_mysql_betterdb, table_department)
    mysql_employee_cnt = mu.get_count(conn_mysql_betterdb, table_employee)
    mysql_job_cnt = mu.get_count(conn_mysql_betterdb, table_job)
    mysql_job_history_cnt = mu.get_count(conn_mysql_betterdb, table_job_history)

    if mysql_department_cnt != oracle_departments_cnt or mysql_employee_cnt != oracle_employees_cnt  or mysql_job_cnt != oracle_jobs_cnt or mysql_job_history_cnt != oracle_job_history_cnt:
        print("Not Valid Count!! mysql_department_cnt = ", mysql_department_cnt, ", oracle_departments_cnt =", oracle_departments_cnt)
        print("Not Valid Count!! mysql_department_cnt = ", mysql_employee_cnt, ", oracle_departments_cnt =", oracle_employees_cnt)
        print("Not Valid Count!! mysql_department_cnt = ", mysql_job_cnt, ", oracle_departments_cnt =", oracle_jobs_cnt)
        print("Not Valid Count!! mysql_department_cnt = ", mysql_job_history_cnt, ", oracle_departments_cnt =", oracle_job_history_cnt)
        exit()

    else:
        print("Count is OK")
        cur = conn_mysql_betterdb.cursor()

        sql = '''select department_id, department_name, manager_id, employee_cnt
                   from Department
                  where department_id = %s
                    and department_name = %s
                    and ifnull(manager_id, '') = ifnull(%s, '')
                    and employee_cnt = %s
                  '''

        cur.executemany(sql, departments_samples)
        sample_rowcount_mysql_department = cur.rowcount

        sql = '''select employee_id, first_name, last_name, email, tel, hire_date, job_id, salary, commission_pct, manager_id, department_id
                   from Employee
                  where employee_id = %s
                    and first_name = %s
                    and last_name = %s
                    and email = %s
                    and tel = %s
                    and hire_date = %s
                    and job_id = %s
                    and salary = %s
                    and ifnull(commission_pct, 0) = ifnull(%s, 0)
                    and ifnull(manager_id, '') = ifnull(%s, '')
                    and ifnull(department_id, '') = ifnull(%s, '')
              '''

        cur.executemany(sql, employees_samples)
        sample_rowcount_mysql_employee = cur.rowcount

        sql = '''select job_id, job_name, min_salary, max_salary
                   from Job
                  where job_id = %s
                    and job_name = %s
                    and min_salary = %s
                    and max_salary = %s
              '''

        cur.executemany(sql, jobs_samples)
        sample_rowcount_mysql_job = cur.rowcount

        sql = '''select employee_id, start_date, end_date, job_id, department_id
                   from Job_history
                  where employee_id = %s
                    and start_date = %s
                    and end_date = %s
                    and job_id = %s
                    and department_id = %s
              '''

        cur.executemany(sql, job_historys_samples)
        sample_rowcount_mysql_job_history = cur.rowcount

        if sample_rowcount_mysql_department == sample_rowcount_oracle_departments and sample_rowcount_mysql_employee == sample_rowcount_oracle_employees and sample_rowcount_mysql_job == sample_rowcount_oracle_jobs and sample_rowcount_mysql_job_history == sample_rowcount_oracle_job_historys:
            print("Whole data is OK", "Verified count of sample_rowcount_mysql_department is", sample_rowcount_mysql_department, "Verified count of sample_rowcount_mysql_employee is", sample_rowcount_mysql_employee)
            
        else:
            print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! Fail", 
              sample_rowcount_mysql_department, sample_rowcount_oracle_departments, sample_rowcount_mysql_employee, sample_rowcount_oracle_employees, sample_rowcount_mysql_job, sample_rowcount_oracle_jobs, sample_rowcount_mysql_job_history, sample_rowcount_oracle_job_historys)