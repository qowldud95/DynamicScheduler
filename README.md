# DynamicScheduler_batch

## 💡 Discriotion
동적 데이터 맵핑 프로그램 (배치) + 동적 스케쥴링

table_mapping 테이블에 맵핑테이블 정보들이 들어있다.\
만약, member1테이블과 member2 테이블이 맵핑되어있다고 가정.\
member1 테이블의 데이터가 member2 테이블에 \
존재하지 않으면 insert(등록) 시켜주고,\
존재한다면 update (수정) 해준다 -> primary key로 비교\
(member1, member2 처럼 정적으로 넣어준 테이블이 아니라, table_mapping 테이블에 맵핑된 정보들은 전부 동적으로 배치처리해준다.)\
이때, 한번의 실행으로 끝나는것이 아니라 (사용자가 지정한) 초마다 배치가 돌도록 동적으로 스케쥴을 걸어준다.

## 💡 Environment
Window,
Java springboot,
Gradle,
H2 database, mysql database, mysql workbench
lombok, jdbc


## 💡prerequisite
- h2 db \
spring.datasource.url=jdbc:h2:tcp://localhost/~/test \
spring.datasource.username=sa \
spring.datasource.password=

![image](https://user-images.githubusercontent.com/77472180/165682729-ab3a8b6e-e969-47d7-9091-a10b6314f85b.png)


- mysql db \
#spring.datasource.driver-class-name=com.mysql.cj.jdbc.Driver \
#spring.datasource.url=jdbc:mysql://localhost:3306/test_db?serverTimezone=UTC&characterEncoding=UTF-8 \
#spring.datasource.username=root \
#spring.datasource.password=1234

![image](https://user-images.githubusercontent.com/77472180/165682841-6bccd73d-892c-48e8-9be6-14bcaa4c2377.png) \
![image](https://user-images.githubusercontent.com/77472180/165682888-2356ba23-3dd8-4d22-b66c-fdaf3234733b.png)

<참고> dml & ddl \
create table column_mapping ( \
table_Key varchar(40), \
source_columnname varchar(1000), \
target_columnname varchar(1000), \
is_primarykey bit, \
PRIMARY KEY (table_Key, source_columnname, target_columnname) \
)

CREATE TABLE TABLE_MAPPING ( \
table_Key varchar(40) primary key, \
source_tableName varchar(100), \
target_tableName varchar(100), \
source_conn varchar(1000), \
source_id varchar(100), \
source_password varchar(200), \
target_conn varchar(1000), \
target_id varchar(100), \
target_password varchar(200) \
)

SET @UU=random_uuid(); \
insert into TABLE_MAPPING(TABLE_KEY,SOURCE_TABLENAME ,TARGET_TABLENAME ,SOURCE_CONN ,SOURCE_ID ,SOURCE_PASSWORD ,TARGET_CONN ,TARGET_ID ,TARGET_PASSWORD ) VALUES(@UU,'MEMBER','MEMBER2','jdbc:h2:tcp://localhost/\~/test','sa','','jdbc:h2:tcp://localhost/~/test','sa','')

SET @UU=random_uuid(); \
insert into TABLE_MAPPING(TABLE_KEY,SOURCE_TABLENAME ,TARGET_TABLENAME ,SOURCE_CONN ,SOURCE_ID ,SOURCE_PASSWORD ,TARGET_CONN ,TARGET_ID ,TARGET_PASSWORD ) VALUES(@UU,'MEMBER','MEMBER3','jdbc:h2:tcp://localhost/~/test','sa','','jdbc:mysql://localhost:3306/test_db?serverTimezone=UTC&characterEncoding=UTF-8','root','1234')

SET @UU=random_uuid(); \
insert into TABLE_MAPPING(TABLE_KEY,SOURCE_TABLENAME ,TARGET_TABLENAME ,SOURCE_CONN ,SOURCE_ID ,SOURCE_PASSWORD ,TARGET_CONN ,TARGET_ID ,TARGET_PASSWORD ) VALUES(@UU,'MEMBER3','MEMBER4','jdbc:mysql://localhost:3306/test_db?serverTimezone=UTC&characterEncoding=UTF-8','root','1234','jdbc:mysql://localhost:3306/test_db?serverTimezone=UTC&characterEncoding=UTF-8','root','1234')

SET @UU=random_uuid(); \
insert into TABLE_MAPPING(TABLE_KEY,SOURCE_TABLENAME ,TARGET_TABLENAME ,SOURCE_CONN ,SOURCE_ID ,SOURCE_PASSWORD ,TARGET_CONN ,TARGET_ID ,TARGET_PASSWORD ) VALUES(@UU,'MEMBER4','MEMBER','jdbc:mysql://localhost:3306/test_db?serverTimezone=UTC&characterEncoding=UTF-8','root','1234','jdbc:h2:tcp://localhost/~/test','sa','')

insert into column_mapping values('aaf44063-fb99-4978-b9cd-8dd94acabfe9','MEMBER_NAME','MEMBER_NAME', 0),('aaf44063-fb99-4978-b9cd-8dd94acabfe9','MEMBER_ID','MEMBER_ID',1),('aaf44063-fb99-4978-b9cd-8dd94acabfe9','MONEY','MONEY', 0)

insert into column_mapping values('a894d858-af0b-42bd-9b9d-15857c57f346','MEMBER_NAME','MEMBER_NAME', 0),('a894d858-af0b-42bd-9b9d-15857c57f346','MEMBER_ID','MEMBER_ID',1),('a894d858-af0b-42bd-9b9d-15857c57f346','MONEY','MONEY', 0)

insert into column_mapping values('03932d34-e9d0-431b-ab65-fd80fe47dbec','MEMBER_NAME','M_NAME', 0),('03932d34-e9d0-431b-ab65-fd80fe47dbec','MEMBER_ID','M_ID',1),('03932d34-e9d0-431b-ab65-fd80fe47dbec','MONEY','M_MONEY', 0)

insert into column_mapping values('c588a3d0-e024-450a-bd08-fd76f94634b6','M_NAME','MEMBER_NAME', 0),('c588a3d0-e024-450a-bd08-fd76f94634b6','M_ID','MEMBER_ID',1),('c588a3d0-e024-450a-bd08-fd76f94634b6','M_MONEY','MONEY', 0)


## 💡 Files
src/main/java/scheduling/dynamicScheduler/controller/AdminController.java \
-> 스케줄러 동작시간 컨트롤 

src/main/java/scheduling/dynamicScheduler/controller/StartController.java \
-> 첫 화면 이어주는 컨트롤러 (index.jsp)

src/main/java/scheduling/dynamicScheduler/dto/ConnectionDTO.java \
-> DBConnection 정보. Url, Id, pw

src/main/java/scheduling/dynamicScheduler/dynamic/DynamicChangeScheduler.java \
-> 스케줄러 동작시간 컨트롤

src/main/java/scheduling/dynamicScheduler/repository/TableRepository.java \
-> DB 테이블 정보 select, insert, update

src/main/java/scheduling/dynamicScheduler/service/ScheduleTask.java \
-> 배치 service 단


