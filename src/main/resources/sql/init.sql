CREATE table results AS SELECT * FROM CSVREAD('classpath:rebellabs-survey-2017-results-20170711-clean.csv', null, 'fieldSeparator=;');


alter table results alter column IDE_RATING DECIMAL(20, 2);
alter table results alter column LANGUAGE_RATING DECIMAL(20, 2);
alter table results alter column APPSTACK_RATING DECIMAL(20, 2);
alter table results alter column ARCHITECTURE_RATING DECIMAL(20, 2);
alter table results alter column DATABASE_RATING DECIMAL(20, 2);

update results set ide = 'Eclipse IDE' where ide = 'Eclipse ';
update results set ide = 'NetBeans IDE' where ide = 'NetBeans';

