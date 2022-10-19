CREATE TABLE query1 AS
SELECT gs.name, COUNT(hg.movieid) AS moviecount
FROM genres gs, hasagenre hg
WHERE gs.genreid = hg.genreid
GROUP BY gs.genreid;

CREATE TABLE query2 AS
SELECT gs.name, AVG(rs.rating) AS rating
FROM genres gs, hasagenre hg, ratings rs
WHERE gs.genreid = hg.genreid AND hg.movieid = rs.movieid
GROUP BY gs.name;

CREATE TABLE query3 AS
SELECT mv.title, COUNT(rs.rating) AS CountOfRatings 
FROM movies mv, ratings rs
WHERE mv.movieid = rs.movieid
GROUP BY mv.title
HAVING COUNT(rs.rating) >= 10;

CREATE TABLE query4 AS
SELECT mv.movieid, mv.title
FROM movies mv, hasagenre hg, genres gs
WHERE gs.name = 'Comedy' AND gs.genreid = hg.genreid AND hg.movieid = mv.movieid;

CREATE TABLE query5 AS
SELECT mv.title, AVG(rs.rating) AS average
FROM movies mv, ratings rs
WHERE mv.movieid = rs.movieid
GROUP BY mv.title;

CREATE TABLE query6 AS
SELECT AVG(rs.rating) AS average
FROM genres gs, hasagenre hg, ratings rs
WHERE gs.name = 'Comedy' AND gs.genreid = hg.genreid AND hg.movieid = rs.movieid;

CREATE TABLE query7 AS
SELECT AVG(rs.rating) AS average
FROM ratings rs
WHERE rs.movieid IN ((
	SELECT hg.movieid
	FROM genres gs, hasagenre hg
	WHERE gs.name = 'Comedy' AND gs.genreid = hg.genreid
) INTERSECT (
	SELECT hg.movieid
	FROM genres gs, hasagenre hg
	WHERE gs.name = 'Romance' AND gs.genreid = hg.genreid
));

CREATE TABLE query8 AS
SELECT AVG(rs.rating) AS average
FROM ratings rs
WHERE rs.movieid IN ((
	SELECT hg.movieid
	FROM genres gs, hasagenre hg
	WHERE gs.name = 'Romance' AND gs.genreid = hg.genreid
) EXCEPT (
	SELECT hg.movieid
	FROM genres gs, hasagenre hg
	WHERE gs.name = 'Comedy' AND gs.genreid = hg.genreid
));

CREATE TABLE query9 AS
SELECT rs.movieid, rs.rating
FROM ratings rs
WHERE rs.userid = :v1;