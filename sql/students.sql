CREATE TABLE IF NOT EXISTS students (
	student_id INT,
	stud_name  VARCHAR(30) NOT NULL,
	room_id    INT,
	sex        CHAR(1) NOT NULL,
	birthday   DATE NOT NULL,
	PRIMARY KEY (student_id),
	FOREIGN KEY (room_id) REFERENCES rooms(room_id)
);