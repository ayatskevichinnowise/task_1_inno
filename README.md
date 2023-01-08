## Task 1 ##

Script that downloads 2 JSON files and upload information into Postgresql database.
The script also gets information from database and saves the results of queries in JSON or XML format.

### Queries: ###
 Markup : 1. List of rooms and number of students in each room
          2. Top 5 rooms with the smallest average student age
          3. Top 5 rooms with the biggest difference in student age
          4. List of rooms where students of different gender

### Options ###

Commands      | Description
------------- | -------------
--rooms_path  | Path to the file with data about rooms  [default:data\rooms.json]
--stud_path   | Path to the file with data about students [default: data\students.json]
--out_path    | Output file path
--out_format  | Output file format  [default: json]
--help        | Show this message and exit.
