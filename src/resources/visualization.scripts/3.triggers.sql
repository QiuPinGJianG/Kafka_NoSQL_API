-- trigger for moving data from temp users table
DROP TRIGGER IF EXISTS events.poll_users;
DELIMITER $$
CREATE TRIGGER events.poll_users AFTER INSERT ON events.users 
FOR EACH ROW 
BEGIN
  IF NEW.user_id NOT IN (SELECT user_id FROM events_dm.users WHERE user_id = NEW.user_id) THEN
    INSERT INTO events_dm.users(
      user_id, 
      locale, 
      birth_year, 
      gender, 
      joined_at, 
      location, 
      time_zone
    ) 
	VALUES(
	  NEW.user_id, 
	  NEW.locale, 
	  NEW.birth_year, 
	  NEW.gender, 
	  NEW.joined_at, 
	  NEW.location, 
	  NEW.time_zone
	);
  ELSE
    UPDATE events_dm.users SET
      locale = NEW.locale, 
      birth_year = NEW.birth_year, 
      gender = NEW.gender, 
      joined_at = NEW.joined_at, 
      location = NEW.location, 
      time_zone = NEW.time_zone
    WHERE user_id = NEW.user_id;
  END IF;
END$$
DELIMITER ;


-- trigger for moving data from temp users table
DROP TRIGGER IF EXISTS events.poll_user_friends_count;
DELIMITER $$
CREATE TRIGGER events.poll_user_friends_count AFTER INSERT ON events.user_friend_count
FOR EACH ROW 
BEGIN
  IF NEW.user_id NOT IN (SELECT user_id FROM events_dm.user_friends_count WHERE user_id = NEW.user_id) THEN
    INSERT INTO events_dm.user_friends_count(
      user_id, 
      friends_count
    ) 
	VALUES(
	  NEW.user_id, 
	  NEW.friend_count
	);
  ELSE
    UPDATE events_dm.user_friends_count SET 
      friends_count = NEW.friend_count
    WHERE user_id = NEW.user_id;
  END IF;
END$$
DELIMITER ;


-- trigger for moving data from temp train table
DROP TRIGGER IF EXISTS events.poll_invitations_from_train;
DELIMITER $$
CREATE TRIGGER events.poll_invitations_from_train AFTER INSERT ON events.train
FOR EACH ROW 
BEGIN
  IF NEW.invited = '0' OR NEW.invited = '1' THEN
    IF NOT EXISTS(SELECT COUNT(*) FROM events_dm.invitations WHERE user_id = NEW.user_id AND event_id = NEW.event_id) THEN
      INSERT INTO events_dm.invitations(
        event_id, 
        user_id, 
        invited, 
        time_stamp
      ) 
  	  VALUES(
  	    NEW.event_id, 
  	    NEW.user_id, 
  	    cast(NEW.invited as unsigned int),
  	    NEW.time_stamp
  	  );
    ELSE
      UPDATE events_dm.invitations SET 
        invited = cast(NEW.invited as unsigned int), 
        time_stamp = NEW.time_stamp
      WHERE user_id = NEW.user_id AND event_id = NEW.event_id;
    END IF;
  END IF;	
END$$
DELIMITER ;
-- trigger for moving data from temp test table
DROP TRIGGER IF EXISTS events.poll_invitations_from_test;
DELIMITER $$
CREATE TRIGGER events.poll_invitations_from_test AFTER INSERT ON events.test
FOR EACH ROW 
BEGIN
  IF NEW.invited = '0' OR NEW.invited = '1' THEN
    IF NOT EXISTS(SELECT COUNT(*) FROM events_dm.invitations WHERE user_id = NEW.user_id AND event_id = NEW.event_id) THEN
      INSERT INTO events_dm.invitations(
        event_id, 
        user_id, 
        invited, 
        time_stamp
      ) 
      VALUES(
        NEW.event_id, 
        NEW.user_id, 
        cast(NEW.invited as unsigned int), 
        NEW.time_stamp
      );
    ELSE
      UPDATE events_dm.invitations SET 
        invited = cast(NEW.invited as unsigned int), 
        time_stamp = NEW.time_stamp
      WHERE user_id = NEW.user_id AND event_id = NEW.event_id;
    END IF;
  END IF;
END$$
DELIMITER ;


-- trigger for moving data from temp events table
DROP TRIGGER IF EXISTS events.poll_events;
DELIMITER $$
CREATE TRIGGER events.poll_events AFTER INSERT ON events.events
FOR EACH ROW 
BEGIN
  IF NOT EXISTS(SELECT COUNT(*) FROM events_dm.events WHERE event_id = NEW.event_id) THEN
    INSERT INTO events_dm.events(
      event_id, 
      host_user_id, 
      start_time, 
      city, 
      state, 
      country, 
      zip_code, 
      latitude, 
      longitude
    )
	VALUES(
	  NEW.event_id, 
	  NEW.user_id, 
	  NEW.start_time, 
	  NEW.city, NEW.state, 
	  NEW.country, 
	  NEW.zip, 
	  NEW.latitude, 
	  NEW.longitude
	);
  ELSE
    UPDATE events_dm.events SET 
      host_user_id = NEW.user_id, 
      start_time = NEW.start_time,
      city = NEW.city, 
      state = NEW.state, 
      country = NEW.country, 
      zip_code = NEW.zip, 
      latitude = NEW.latitude, 
      longitude = NEW.longitude
    WHERE event_id = NEW.event_id;
  END IF;
END$$
DELIMITER ;



-- trigger for moving data from temp event_attend_count table
DROP TRIGGER IF EXISTS events.poll_event_attendees_count;
DELIMITER $$
CREATE TRIGGER events.poll_event_attendees_count AFTER INSERT ON events.event_attendee_count
FOR EACH ROW 
BEGIN
  IF EXISTS(SELECT COUNT(*) FROM events_dm.event_attendees_count WHERE event_id = NEW.event_id) THEN
    INSERT INTO events_dm.event_attendees_count(
      event_id, 
      attendees_count, 
      attend_type
    )
	VALUES(
	  NEW.event_id, 
	  NEW.attend_count, 
	  NEW.attend_type
	);
  ELSE
    UPDATE events_dm.event_attendees_count SET 
      attendees_count = NEW.attend_count, 
      attend_type = NEW.attend_type
    WHERE event_id = NEW.event_id;
  END IF;
END$$
DELIMITER ;



-- trigger for moving data from temp predictions table
DROP TRIGGER IF EXISTS events.poll_predictions;
DELIMITER $$
CREATE TRIGGER events.poll_predictions AFTER INSERT ON events.predictions
FOR EACH ROW 
BEGIN
  IF EXISTS(SELECT COUNT(*) FROM events_dm.predictions WHERE user_id = NEW.user_id AND event_id = NEW.event_id) THEN
    INSERT INTO events_dm.predictions(
      event_id, 
      user_id, 
      interested, 
      creation_time
    ) 
	VALUES(
	  NEW.event_id, 
	  NEW.user_id, 
	  NEW.user_interested, 
	  NEW.prediction_time
	);
  ELSE
    UPDATE events_dm.predictions SET 
      interested = NEW.user_interested, 
      creation_time = NEW.prediction_time
    WHERE user_id = NEW.user_id AND event_id = NEW.event_id;
  END IF;
END$$
DELIMITER ;
