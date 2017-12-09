show databases;


-- 在mysql中重新创建数据库：test4
create database test4;

use test4;

show tables;






-- */
-- --创建测试数据

-- --1.学生表
-- Student(S,Sname,Sage,Ssex) --S 学生编号,Sname 学生姓名,Sage 出生年月,Ssex 学生性别
CREATE TABLE Student(S VARCHAR(10),Sname VARCHAR(10),Sage VARCHAR(20),Ssex VARCHAR(10));
INSERT INTO Student VALUES('01' , '赵雷' , '1990-01-01' , '男');
INSERT INTO Student VALUES('02' , '钱电' , '1990-12-21' , '男');
INSERT INTO Student VALUES('03' , '孙风' , '1990-05-20' , '男');
INSERT INTO Student VALUES('04' , '李云' , '1990-08-06' , '男');
INSERT INTO Student VALUES('05' , '周梅' , '1991-12-01' , '女');
INSERT INTO Student VALUES('06' , '吴兰' , '1992-03-01' , '女');
INSERT INTO Student VALUES('07' , '郑竹' , '1989-07-01' , '女');
INSERT INTO Student VALUES('08' , '王菊' , '1990-01-20' , '女');

-- --2.课程表 
-- Course(C,Cname,T) --C --课程编号,Cname 课程名称,T 教师编号
CREATE TABLE Course(C VARCHAR(10),Cname VARCHAR(10),T VARCHAR(10));
INSERT INTO Course VALUES('01' , '语文' , '02');
INSERT INTO Course VALUES('02' , '数学' , '01');
INSERT INTO Course VALUES('03' , '英语' , '03');

-- --3.教师表 
-- Teacher(T,Tname) --T 教师编号,Tname 教师姓名
CREATE TABLE Teacher(T VARCHAR(10),Tname VARCHAR(10));
INSERT INTO Teacher VALUES('01' , '张三');
INSERT INTO Teacher VALUES('02' , '李四');
INSERT INTO Teacher VALUES('03' , '王五');

-- --4.成绩表 
-- SC(S,C,score) --S 学生编号,C 课程编号,score 分数
CREATE TABLE SC(S VARCHAR(10),C VARCHAR(10),score DECIMAL(18,1));
INSERT INTO SC VALUES('01' , '01' , 80);
INSERT INTO SC VALUES('01' , '02' , 90);
INSERT INTO SC VALUES('01' , '03' , 99);
INSERT INTO SC VALUES('02' , '01' , 70);
INSERT INTO SC VALUES('02' , '02' , 60);
INSERT INTO SC VALUES('02' , '03' , 80);
INSERT INTO SC VALUES('03' , '01' , 80);
INSERT INTO SC VALUES('03' , '02' , 80);
INSERT INTO SC VALUES('03' , '03' , 80);
INSERT INTO SC VALUES('04' , '01' , 50);
INSERT INTO SC VALUES('04' , '02' , 30);
INSERT INTO SC VALUES('04' , '03' , 20);
INSERT INTO SC VALUES('05' , '01' , 76);
INSERT INTO SC VALUES('05' , '02' , 87);
INSERT INTO SC VALUES('06' , '01' , 31);
INSERT INTO SC VALUES('06' , '03' , 34);
INSERT INTO SC VALUES('07' , '02' , 89);
INSERT INTO SC VALUES('07' , '03' , 98);

1、查询学过课程编号为"01"和"02"，但是没有学过"03"课程的同学的信息

select * 
from  SC 
where C='03'

select S from SC where C='01'

select S from SC where C='02'


select distinct S from SC a 
	where a.S in (select S from SC where C='01') 
				and a.S in (select S from SC where C='02')
				and a.S not in (select S from SC where C='03')


)


2、查询一门及其以上课程的成绩大于等于90同学的学号，姓名及其平均成绩和总成绩

select S, sum(score), avg(score) from SC group by S

select a.S,b.Sname, c.sum, c.avg from SC a 
	left join student b on b.S=a.S
	left join (
		select S, sum(score) sum , avg(score) avg from SC group by S
	) c on c.S=b.S
where score>=90 





1. 查询选修语文课程的学生中，语文成绩最高的学生信息及其语文成绩

select C from course where Cname='语文'


select * from student a 
	right join (
			select S, max(score) max_score from SC where C=(select C from course where Cname='语文')
		) b 
		on a.S=b.S
	









































