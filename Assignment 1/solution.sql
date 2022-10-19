DROP TABLE IF EXISTS users CASCADE;
DROP TABLE IF EXISTS movies CASCADE;
DROP TABLE IF EXISTS taginfo CASCADE;
DROP TABLE IF EXISTS genres CASCADE;
DROP TABLE IF EXISTS ratings CASCADE;
DROP TABLE IF EXISTS tags CASCADE;
DROP TABLE IF EXISTS hasagenre CASCADE;

CREATE TABLE users(
userid INTEGER,
name TEXT NOT NULL,
PRIMARY KEY (userid)
);

CREATE TABLE movies(
movieid INTEGER,
title TEXT NOT NULL,
PRIMARY KEY (movieid)
);

CREATE TABLE ratings(
userid INTEGER,
movieid INTEGER,
rating NUMERIC CHECK(rating >= 0.0 and rating <= 5.0),
timestamp BIGINT NOT NULL,
PRIMARY KEY (userid, movieid),
FOREIGN KEY (userid) REFERENCES users ON DELETE CASCADE,
FOREIGN KEY (movieid) REFERENCES movies ON DELETE CASCADE
);

CREATE TABLE taginfo(
tagid INTEGER,
content TEXT NOT NULL,
PRIMARY KEY (tagid)
);

CREATE TABLE tags(
userid INTEGER,
movieid INTEGER,
tagid INTEGER,
timestamp BIGINT NOT NULL,
PRIMARY KEY (userid, movieid, tagid),
FOREIGN KEY (userid) REFERENCES users ON DELETE CASCADE,
FOREIGN KEY (tagid) REFERENCES taginfo ON DELETE CASCADE,
FOREIGN KEY (movieid) REFERENCES movies ON DELETE CASCADE
);

CREATE TABLE genres(
genreid INTEGER,
name TEXT NOT NULL,
PRIMARY KEY (genreid)
);

CREATE TABLE hasagenre(
movieid INTEGER,
genreid INTEGER,
PRIMARY KEY (genreid, movieid),
FOREIGN KEY (genreid) REFERENCES genres ON DELETE CASCADE,
FOREIGN KEY (movieid) REFERENCES movies ON DELETE CASCADE
);

-- This will only be used for testing purposes only GRANT PERMISSIONS TO FOLDER
copy users from 'C:/Users/Aman-ASU/My Graduate/Data Processing at Scale/Assignments/Coursera-ASU-Database-master/course1/assignment1/exampleinput/users.dat' DELIMITERS '%';

copy movies from 'C:/Users/Aman-ASU/My Graduate/Data Processing at Scale/Assignments/Coursera-ASU-Database-master/course1/assignment1/exampleinput/movies.dat' DELIMITERS '%';

copy taginfo from 'C:/Users/Aman-ASU/My Graduate/Data Processing at Scale/Assignments/Coursera-ASU-Database-master/course1/assignment1/exampleinput/taginfo.dat' DELIMITERS '%';

copy genres from 'C:/Users/Aman-ASU/My Graduate/Data Processing at Scale/Assignments/Coursera-ASU-Database-master/course1/assignment1/exampleinput/genres.dat' DELIMITERS '%';

copy ratings from 'C:/Users/Aman-ASU/My Graduate/Data Processing at Scale/Assignments/Coursera-ASU-Database-master/course1/assignment1/exampleinput/ratings.dat' DELIMITERS '%';

copy tags from 'C:/Users/Aman-ASU/My Graduate/Data Processing at Scale/Assignments/Coursera-ASU-Database-master/course1/assignment1/exampleinput/tags.dat' DELIMITERS '%';

copy hasagenre from 'C:/Users/Aman-ASU/My Graduate/Data Processing at Scale/Assignments/Coursera-ASU-Database-master/course1/assignment1/exampleinput/hasagenre.dat' DELIMITERS '%';