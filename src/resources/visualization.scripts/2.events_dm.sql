-- create database 
CREATE DATABASE IF NOT EXISTS events_dm;

-- create users table
DROP TABLE IF EXISTS events_dm.users;
CREATE TABLE IF NOT EXISTS events_dm.users(
	user_id varchar(32),
	locale varchar(8),
	birth_year int,
	gender varchar(8),
	joined_at varchar(32),
	location varchar(128),
	time_zone varchar(8),
	PRIMARY KEY(user_id)
);

-- create user_friends_count table
DROP TABLE IF EXISTS events_dm.user_friends_count;
CREATE TABLE IF NOT EXISTS events_dm.user_friends_count(
  user_id varchar(32),
  friends_count int,
  PRIMARY KEY(user_id)
);

-- create invitations table
DROP TABLE IF EXISTS events_dm.invitations;
CREATE TABLE IF NOT EXISTS events_dm.invitations(
  event_id varchar(32),
  user_id varchar(32),
  invited int,
  time_stamp varchar(32),
  PRIMARY KEY(event_id, user_id)
);

-- create event_schdule_time table 
DROP TABLE IF EXISTS events_dm.event_schedule_time;
CREATE TABLE IF NOT EXISTS events_dm.event_schedule_time(
  schedule_id int,
  start_time TIME,
  end_time TIME,
  PRIMARY KEY(schedule_id)
); 
-- schedule data
INSERT INTO events_dm.event_schedule_time(schedule_id, start_time, end_time) VALUES(0, '00:00:00', '08:00:00');
INSERT INTO events_dm.event_schedule_time(schedule_id, start_time, end_time) VALUES(1, '08:00:00', '09:00:00');
INSERT INTO events_dm.event_schedule_time(schedule_id, start_time, end_time) VALUES(2, '09:00:00', '10:00:00');
INSERT INTO events_dm.event_schedule_time(schedule_id, start_time, end_time) VALUES(3, '10:00:00', '11:00:00');
INSERT INTO events_dm.event_schedule_time(schedule_id, start_time, end_time) VALUES(4, '11:00:00', '13:00:00');
INSERT INTO events_dm.event_schedule_time(schedule_id, start_time, end_time) VALUES(5, '13:00:00', '14:00:00');
INSERT INTO events_dm.event_schedule_time(schedule_id, start_time, end_time) VALUES(6, '14:00:00', '15:00:00');
INSERT INTO events_dm.event_schedule_time(schedule_id, start_time, end_time) VALUES(7, '15:00:00', '17:00:00');
INSERT INTO events_dm.event_schedule_time(schedule_id, start_time, end_time) VALUES(8, '17:00:00', '21:00:00');
INSERT INTO events_dm.event_schedule_time(schedule_id, start_time, end_time) VALUES(9, '21:00:00', '23:59:59');

-- create event_schdule_weekday
DROP TABLE IF EXISTS events_dm.event_schedule_weekday;
CREATE TABLE IF NOT EXISTS events_dm.event_schedule_weekday(
  weekday_id int,
  weekday_name varchar(16),
  PRIMARY KEY(weekday_id)
); 
-- weekday data
INSERT INTO events_dm.event_schedule_weekday(weekday_id, weekday_name) VALUES
  (2, 'Monday'), (3, 'Tuesday'), (4, 'Wednesday'), (5, 'Thursday'), (6, 'Friday'), (7, 'Saturday'), (1, 'Sunday');

-- create event_schedule_month
DROP TABLE IF EXISTS events_dm.event_schedule_month;
CREATE TABLE IF NOT EXISTS events_dm.event_schedule_month(
  month_id varchar(2),
  month_name varchar(16),
  PRIMARY KEY(month_id)
); 
-- data
INSERT INTO events_dm.event_schedule_month(month_id, month_name) VALUES
  ('01', 'January'), ('02', 'Febuary'), ('03', 'March'), ('04', 'April'), ('05', 'May'), ('06', 'June'), ('07', 'July'), ('08', 'August'), ('09', 'September'), ('10', 'October'), ('11', 'November'), ('12', 'December');
  
-- create invitation_days table
DROP TABLE IF EXISTS events_dm.invitation_duration;
CREATE TABLE IF NOT EXISTS events_dm.invitation_duration(
  duration_id int,
  min_days int,
  max_days int,
  PRIMARY KEY(duration_id)
);
-- data
INSERT INTO events_dm.invitation_duration(duration_id, min_days, max_days) VALUES
  (0, 0, 3), (1, 3, 7), (2, 7, 15), (3, 15, 21), (4, 21, 30), (5, 30, 60), (6, 60, 90), (7, 90, 150), (8, 150, 360);
  
-- CREATE TABLE events
DROP TABLE IF EXISTS events_dm.events;
CREATE TABLE IF NOT EXISTS events_dm.events(
	event_id varchar(32),
	host_user_id varchar(64),
	start_time varchar(32),
	city varchar(256),             	                    
	state varchar(64),
	country varchar(128),              	                    
	zip_code varchar(256),
	latitude float,
	longitude float,
	PRIMARY KEY(event_id)
);

-- create event_attendees_count table
DROP TABLE IF EXISTS events_dm.event_attendees_count;
CREATE TABLE IF NOT EXISTS events_dm.event_attendees_count(
  event_id varchar(32),
  attendees_count int,
  attend_type varchar(8),
  PRIMARY KEY(event_id, attend_type)
);

-- create predictions tables
DROP TABLE IF EXISTS events_dm.predictions;
CREATE TABLE IF NOT EXISTS events_dm.predictions(
  event_id varchar(32),
  user_id varchar(32),
  interested int,
  creation_time varchar(32),
  PRIMARY KEY(event_id, user_id)
);


