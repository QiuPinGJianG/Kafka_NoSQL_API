-- get month function
DROP FUNCTION IF EXISTS get_month;
CREATE FUNCTION get_month(dts varchar(32)) returns varchar(2) return substring(dts, 6, 2);

-- get time function
DROP FUNCTION IF EXISTS get_time;
CREATE FUNCTION get_time(dts varchar(32)) returns TIME return STR_TO_DATE(substring(dts, 12, 8), '%H:%i:%s');

-- get weekday function
DROP FUNCTION IF EXISTS get_day_of_week;
CREATE FUNCTION get_day_of_week(dts varchar(32)) returns int return DAYOFWEEK(substring(dts, 1, 10));

-- get weekday function
DROP FUNCTION IF EXISTS get_date;
CREATE FUNCTION get_date(dts varchar(32)) returns DATE return STR_TO_DATE(substring(dts, 1, 10), '%Y-%m-%d');

-- get weekday function
DROP FUNCTION IF EXISTS get_age;
CREATE FUNCTION get_age(dts varchar(32)) returns int return YEAR(now()) - YEAR(get_date(dts));

-- get days function
DROP FUNCTION IF EXISTS get_days;
CREATE FUNCTION get_days(dts1 varchar(32), dts2 varchar(32)) returns int return datediff(get_date(dts2), get_date(dts1));



-- create events_view
DROP VIEW IF EXISTS events_dm.v_events;
CREATE VIEW events_dm.v_events AS 
SELECT
  event_id,
  host_user_id,
  start_time AS start_date_time,
  get_time(start_time) AS start_time,
  get_day_of_week(start_time) AS start_day_of_week,
  get_month(start_time) AS start_month,
  city,
  state,
  country,
  zip_code,
  latitude,
  longitude
FROM events_dm.events;

-- create users_view
DROP VIEW IF EXISTS events_dm.v_users;
CREATE VIEW events_dm.v_users AS 
SELECT
  user_id,
  locale,
  (YEAR(now()) - CONVERT(birth_year, UNSIGNED INTEGER)) AS age,
  gender,
  get_age(joined_at) AS member_age,
  location,
  time_zone
FROM events_dm.users;


-- create invitations_view
DROP VIEW IF EXISTS events_dm.v_invitations;
CREATE VIEW events_dm.v_invitations AS 
SELECT
  e.event_id,
  i.user_id,
  i.invited,
  get_days(i.time_stamp, e.start_time) AS ahead_days
FROM events_dm.invitations i
  INNER JOIN events_dm.events e ON i.event_id = e.event_id;
