mysql_parallel_load - Multi-threaded "LOAD DATA INFILE"

Requirements:

- mysql-connector
- filecleaver (https://github.com/fprimex/filecleaver)

Usage: python3 mysql_parallel_load.py csv_file destination_table number_of_threads

You can test the program with the provided files:
- test_db.sql contains the schema and test table definition.
- dummy_data.tar.bz2 contains the data to run the test. This file can be found here:
    - https://drive.google.com/file/d/1_3F7XcEJMWFJ0i79xgfnb0M8wySLMnh6/view?usp=sharing
    
After sourcing the SQL file, run with: python3 mysql_parallel_load.py dummy_data.csv test_load 4

In my own tests, best performance was achieved with 4 load threads.

*** IMPORTANT ***
If you are using InnoDB, you need to set innodb_autoinc_lock_mode to 2, or else each "LOAD DATA INFILE " will lock entire table.
When you use this AUTO_INCREMENT mode, there are some caveats for statement-based replication.
Check https://dev.mysql.com/doc/refman/5.7/en/innodb-auto-increment-handling.html for the details ("InnoDB AUTO_INCREMENT Lock Mode Usage Implications").