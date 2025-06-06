import random
import string

# Starting ID after the provided data
start_id = 1
num_rows = 30
output_file = "inserts.sql"

test_cases = """
show tables;

describe t;
describe t1;

drop table t1;
show tables;

explain select t.id, t.name from t where t.age > 18;

select t.id, t.name from t where t.age > 18;

select * from t where t.age = 18 or t.age = 19;
select * from t where t.age > 18 ;
select * from t where t.age >= 18 ;
select * from t where t.age < 18 ;
select * from t where t.age <= 18 ;
select * from t where t.age != 18 ;

select * from t where t.age < 18 or t.age > 19;

delete from t where t.age = 18;
select * from t where t.age = 18;

delete from t;
select * from t;

select min(t.id),max(t.id),sum(t.id),count(t.id),avg(t.id) from t;


select sum(t.age),min(t.age),max(t.age),count(t.age),avg(t.age) from t group by t.age;
select sum(t.id),min(t.id),max(t.id),count(t.id),avg(t.id) from t group by t.id;

select sum(t.age),min(t.age),max(t.age),count(t.age),avg(t.age),t.age from t group by t.age order by t.age;

select sum(t.id),min(t.id),max(t.id),count(t.id),avg(t.id),t.id from t group by t.id order by t.id;

select sum(t.id),min(t.id),max(t.id),count(t.id),avg(t.id),t.id from t group by t.id order by t.id asc;

select sum(t.id),min(t.id),max(t.id),count(t.id),avg(t.id),t.id from t group by t.id order by t.id desc;

drop table Students;
drop table Courses;

CREATE TABLE Students (
    StudentID INT,
    StudentName CHAR
);

CREATE TABLE Courses (
    CourseID INT,
    CourseName CHAR,
    StudentID INT
);

INSERT INTO Students (StudentID, StudentName) VALUES
(1, 'Alice'),
(2, 'Bob'),
(3, 'Charlie'),
(4, 'David');

INSERT INTO Courses (CourseID, CourseName, StudentID) VALUES
(101, 'Math', 1),
(102, 'Science', 2),
(103, 'History', 1),
(104, 'Art', 3);

SELECT Students.StudentName, Courses.CourseName
FROM Students
INNER JOIN Courses
ON Students.StudentID = Courses.StudentID;

SELECT Students.StudentName, Courses.CourseName
FROM Students
LEFT JOIN Courses
ON Students.StudentID = Courses.StudentID;

SELECT Students.StudentName, Courses.CourseName
FROM Students
RIGHT JOIN Courses
ON Students.StudentID = Courses.StudentID;

SELECT Students.StudentName, Courses.CourseName
FROM Students
CROSS JOIN Courses

select * from t1;
alter table t1 drop column age;
select * from t1;
desc t1;

create index idx_t_id on t(id);
create index idx_t_age on t(age);

show btree t id;
show btree t age;

drop index idx_t_id;
drop index idx_t_age;

show btree t id;
show btree t age;


"""


# Generate random names (2-3 letters)
def random_name(length):
    return "".join(random.choices(string.ascii_lowercase, k=length))


# Open file to write SQL statements
with open(output_file, "w") as f:
    f.write("drop table t; \ncreate table t (id int,name CHAR,age int,gpa float);\n")
    f.write("insert into t (id, name, age, gpa) values ")
    for i in range(start_id, start_id + num_rows):
        name = random_name(random.randint(2, 3))
        age = random.randint(15, 25)
        gpa = round(random.uniform(2.0, 4.0), 2)
        f.write(f"({i}, '{name}', {age}, {gpa}),")
    f.seek(f.tell() - 1, 0)
    f.write(";\n")

    # Open file to write SQL statements
    f.write("drop table t1; \ncreate table t1 (id int,name CHAR,age int,gpa float);\n")
    f.write("insert into t1 (id, name, age, gpa) values ")
    for i in range(start_id, start_id + num_rows):
        name = random_name(random.randint(2, 3))
        age = random.randint(15, 25)
        gpa = round(random.uniform(2.0, 4.0), 2)
        f.write(f"({i}, '{name}', {age}, {gpa}),")
    f.seek(f.tell() - 1, 0)
    f.write(";\n")

    # Write test cases
    f.write(test_cases)
