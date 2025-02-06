/*******************************************************************************
					CREATING DATABASE AND DATA CLEANING
********************************************************************************/


/*******************************************************************************
   Drop database if it exists
********************************************************************************/
#DROP DATABASE IF EXISTS `art_museum`;

/*******************************************************************************
   Create a new database
********************************************************************************/
CREATE DATABASE `art_museum`;

/*******************************************************************************
   Create table to import values from a csv file
********************************************************************************/
DROP TABLE IF EXISTS artworks;
CREATE TABLE artworks (
	Object_ID BIGINT NOT NULL PRIMARY KEY,
	Object_Number TEXT DEFAULT NULL,
	Is_Highlight TEXT DEFAULT NULL,
	Is_Public_Domain TEXT DEFAULT NULL,
	Gallery_Number TEXT DEFAULT NULL,
	Department TEXT DEFAULT NULL,
	AccessionYear INT DEFAULT NULL,
	Object_Name TEXT DEFAULT NULL,
	Title TEXT DEFAULT NULL,
	Culture TEXT DEFAULT NULL,
	Period TEXT DEFAULT NULL,
	Dynasty TEXT DEFAULT NULL,
	Reign TEXT DEFAULT NULL,
	Portfolio TEXT DEFAULT NULL,
	Artist_Role TEXT DEFAULT NULL,
	Artist_Prefix TEXT DEFAULT NULL,
	Artist_Display_Name TEXT DEFAULT NULL,
	Artist_Display_Bio TEXT DEFAULT NULL,
	Artist_Suffix TEXT DEFAULT NULL,
	Artist_Alpha_Sort TEXT DEFAULT NULL,
	Artist_Nationality TEXT DEFAULT NULL,
	Artist_Begin_Date TEXT DEFAULT NULL,
	Artist_End_Date TEXT DEFAULT NULL,
	Artist_Gender TEXT DEFAULT NULL,
	Object_Date TEXT DEFAULT NULL,
	Object_Begin_Date INT DEFAULT NULL,
	Object_End_Date INT DEFAULT NULL,
	Medium TEXT DEFAULT NULL,
	Dimensions TEXT DEFAULT NULL,
	Credit_Line TEXT DEFAULT NULL,
	Geography_Type TEXT DEFAULT NULL,
	City TEXT DEFAULT NULL,
	State TEXT DEFAULT NULL,
	County TEXT DEFAULT NULL,
	Country TEXT DEFAULT NULL,
	Region TEXT DEFAULT NULL,
	Subregion TEXT DEFAULT NULL,
	Locale TEXT DEFAULT NULL,
	Locus TEXT DEFAULT NULL,
	Excavation TEXT DEFAULT NULL,
	River TEXT DEFAULT NULL,
	Classification TEXT DEFAULT NULL,
	Rights_and_Reproduction TEXT DEFAULT NULL,
	Repository TEXT DEFAULT NULL,
	Tags TEXT DEFAULT NULL
);

/*******************************************************************************
   Import data using MySQL Command Line Client
********************************************************************************/
# SET GLOBAL local_infile = true;
/*******************************************************************************
   Command Line Prompt:
   
   LOAD DATA LOCAL INFILE 'file_path.csv'
   INTO TABLE artworks
   FIELDS TERMINATED BY ','
   ENCLOSED BY '"'
   LINES TERMINATED BY '\n'
   IGNORE 1 ROWS;
********************************************************************************/
SELECT * FROM artworks;

/*******************************************************************************
	Set Charset
********************************************************************************/
ALTER TABLE `art_museum`.`artworks` 
CHARACTER SET = utf8mb4;


SELECT count(*)
FROM information_schema.columns
WHERE table_name = 'artworks'
;

DESCRIBE artworks;

/*******************************************************************************
	Create a duplicate table as a reference source
********************************************************************************/
CREATE TABLE artworks_ref AS SELECT * FROM artworks;

-- Create another `artworks` table if mistakes are made
DROP TABLE IF EXISTS artworks;
CREATE TABLE artworks AS SELECT * FROM artworks_ref;  


/*******************************************************************************
	Drops duplicate and unnecessary columns:
    Artist_Role;
    Artist_Prefix,
    Artist_Display_Bio,
    Artist_Suffix,
    Artist_Alpha_Sort,
    Artist_Gender,
    Object_Date,
    Credit_Line,
    Rights_and_Reproduction,
    Repository
********************************************************************************/
ALTER TABLE artworks
	DROP Artist_Role,
	DROP Artist_Prefix,
    DROP Artist_Display_Bio,
	DROP Artist_Suffix,
	DROP Artist_Alpha_Sort,
    DROP Artist_Gender,
    DROP Object_Date,
    DROP Credit_Line,
    DROP Rights_and_Reproduction,
    DROP Repository;


/*******************************************************************************
	Columns with "|" (e.g: 'Designer|Manufacturer')
********************************************************************************/

/*******************************************************************************
	Group 1:
	Artist_Display_Name,
	Artist_Nationality,
	Artist_Begin_Date,
	Artist_End_Date
    
    Group 2:
    Geography_Type,
	City,
	State,
	County,
	Country,
	Region,
	Subregion,
	Locale,
	Locus,
	Excavation, 
    River
    
    Group 3:
    Classification
    
    Group 4:
    Tags
********************************************************************************/

# Create a Stored Procedure that can split up string into rows & separate each column into table (similar to Pivot Table)

DELIMITER $$

CREATE PROCEDURE SplitString(IN column_name VARCHAR(50), 
							 IN display_name VARCHAR(50),
                             IN table_name VARCHAR(50))
BEGIN
    -- Drop table if it exists
    SET @drop_table = CONCAT('DROP TABLE IF EXISTS ', table_name, ';');
    PREPARE drop_stmt FROM @drop_table;
    EXECUTE drop_stmt;
    DEALLOCATE PREPARE drop_stmt;

    -- Create a table to hold results of the procedure
    SET @create_table = CONCAT(
        'CREATE TABLE ', table_name, ' (Object_ID BIGINT, ', display_name, ' TEXT, pos INT, PRIMARY KEY (Object_ID, pos));'
    );
    PREPARE create_stmt FROM @create_table;
    EXECUTE create_stmt;
    DEALLOCATE PREPARE create_stmt;

    -- Create the procedure for splitting strings
    SET @string = CONCAT(
        'WITH RECURSIVE SplitCTE AS (
            SELECT Object_ID,
                SUBSTRING_INDEX(', column_name, ', "|", 1) AS ', display_name, ',
                SUBSTRING(', column_name, ', CHAR_LENGTH(SUBSTRING_INDEX(', column_name, ', "|", 1)) + 2) AS `Remain`,
                1 AS pos
            FROM artworks
            
            UNION ALL
            
            SELECT Object_ID,
                SUBSTRING_INDEX(Remain, "|", 1),
                SUBSTRING(Remain, CHAR_LENGTH(SUBSTRING_INDEX(Remain, "|", 1)) + 2),
                pos + 1
            FROM SplitCTE
            WHERE Remain != ""
        )
        SELECT Object_ID, ', display_name, ', pos
        FROM SplitCTE
        ORDER BY Object_ID;'
    );

    -- Insert values into the table
    SET @insert_into_tbl = CONCAT('INSERT INTO ', table_name, ' (Object_ID, ', display_name, ', pos) ', @string);
    PREPARE insert_stmt FROM @insert_into_tbl;
    EXECUTE insert_stmt;
    DEALLOCATE PREPARE insert_stmt;

END $$

DELIMITER ;

/*******************************************************************************
	Creating Tables for the columns
********************************************************************************/

/*******************************************************************************
	GROUP 1
********************************************************************************/

-- Artitst Name
CALL SplitString('Artist_Display_Name', 'Artist_Name', 'Artist_Name');


-- Artist_Nationality
CALL SplitString('Artist_Nationality', 'Artist_Nationality', 'Artist_Nationality');


-- Artist_Begin_Date
CALL SplitString('Artist_Begin_Date', 'Artist_Begin_Date', 'Artist_Begin_Date');


-- Artist_End_Date
CALL SplitString('Artist_End_Date', 'Artist_End_Date', 'Artist_End_Date');



-- Join all the above tables to create an 'Artist' table

DROP TABLE IF EXISTS artist;
CREATE TABLE artist AS
	SELECT ROW_NUMBER () OVER () AS Artist_ID,    # Add Artist_ID at the first column
	   Artist_Name, Artist_Begin_Date, Artist_End_Date, Artist_Nationality
	FROM
		(SELECT DISTINCT Artist_Name, Artist_Begin_Date, Artist_End_Date, Artist_Nationality
						FROM artist_name t1
			LEFT JOIN artist_begin_date t2 ON t1.Object_ID = t2.Object_ID AND t1.pos = t2.pos
			LEFT JOIN artist_end_date t3 ON t1.Object_ID = t3.Object_ID AND t1.pos = t3.pos
			LEFT JOIN artist_nationality t4 ON t1.Object_ID = t4.Object_ID AND t1.pos = t4.pos			
		WHERE Artist_Name != ''
		ORDER BY Artist_Name) joined_table;

-- Add Primary Key to 'artist' Table 
ALTER TABLE artist
MODIFY Artist_ID BIGINT;  # Assigned data type

ALTER TABLE artist
ADD CONSTRAINT PK_Artist_ID PRIMARY KEY (Artist_ID);


-- Create a link table between `artist` and `artworks`: `artist_list`
DROP TABLE IF EXISTS artist_list;
CREATE TABLE artist_list AS
SELECT ROW_NUMBER () OVER (ORDER BY Object_ID, Artist_Name) AS Artistlist_ID,  # Add Artist_ID at the first column
	   Object_ID, Artist_Name, Artist_ID
	FROM
		(SELECT t1.Object_ID, t1.Artist_Name, Artist_ID
		FROM artist_name t1	
			LEFT JOIN artist_begin_date t2 ON t1.Object_ID = t2.Object_ID AND t1.pos = t2.pos
			LEFT JOIN artist_end_date t3 ON t1.Object_ID = t3.Object_ID AND t1.pos = t3.pos
			LEFT JOIN artist_nationality t4 ON t1.Object_ID = t4.Object_ID AND t1.pos = t4.pos
			LEFT JOIN artist t5 ON t1.Artist_Name = t5.Artist_Name 					    
								AND t2.Artist_Begin_Date = t5.Artist_Begin_Date
								AND t3.Artist_End_Date = t5.Artist_End_Date
								AND t4.Artist_Nationality = t5.Artist_Nationality                        
		ORDER BY t1.Object_ID, t1.Artist_Name) joined_table;


-- Drop existing tables 
DROP TABLE Artist_Role, Artist_Name, Artist_Nationality, 
		Artist_Begin_Date, Artist_End_Date,	Artist_Gender;
        
-- Add Primary Key to the `artist_list` Table
ALTER TABLE artist_list
MODIFY COLUMN Artistlist_ID BIGINT;

ALTER TABLE artist_list
ADD CONSTRAINT PK_Artistlist_ID
PRIMARY KEY (Artistlist_ID);


-- Add Foreign Keys to the `artist_list` Table
ALTER TABLE artist_list
ADD CONSTRAINT FK_Artworks_Object
FOREIGN KEY (Object_ID) REFERENCES artworks (Object_ID);

ALTER TABLE artist_list
ADD CONSTRAINT FK_Artist_ArtistID
FOREIGN KEY (Artist_ID) REFERENCES artist (Artist_ID);



/*******************************************************************************
	GROUP 2
********************************************************************************/
-- Geography_Type
CALL SplitString('Geography_Type', 'Geography_Type', 'Geography_Type');

-- City
CALL SplitString('City', 'City', 'City');

-- State
CALL SplitString('State', 'State', 'State');

-- County
CALL SplitString('County', 'County', 'County');

-- Country
CALL SplitString('Country', 'Country', 'Country');

-- Region
CALL SplitString('Region', 'Region', 'Region');

-- Subregion
CALL SplitString('Subregion', 'Subregion', 'Subregion');

-- Locale
CALL SplitString('Locale', 'Locale', 'Locale');

-- Locus
CALL SplitString('Locus', 'Locus', 'Locus');

-- Excavation
CALL SplitString('Excavation', 'Excavation', 'Excavation');

-- River
CALL SplitString('River', 'River', 'River');


-- Join all the above tables to create an 'Geography' table
DROP TABLE IF EXISTS geography;

CREATE TABLE geography AS
SELECT ROW_NUMBER () OVER () AS Geo_ID,
		t1.Object_ID, Geography_Type, 
	   COALESCE(City, '') AS City,  
       COALESCE(State, '') AS State, 
       COALESCE(County, '') AS County, 
       COALESCE(Country, '') AS Country, 
       COALESCE(Region, '') AS Region,
       COALESCE(Subregion, '') AS Subregion,
       COALESCE(Locale, '') AS Locale,
       COALESCE(Locus, '') AS Locus,
       COALESCE(Excavation, '') AS Excavation,
       River
FROM Geography_Type t1
	LEFT JOIN City t2 ON t1.Object_ID = t2.Object_ID AND t1.pos = t2.pos
    LEFT JOIN State t3 ON t1.Object_ID = t3.Object_ID AND t1.pos = t3.pos
    LEFT JOIN County t4 ON t1.Object_ID = t4.Object_ID AND t1.pos = t4.pos
    LEFT JOIN Country t5 ON t1.Object_ID = t5.Object_ID AND t1.pos = t5.pos
    LEFT JOIN Region t6 ON t1.Object_ID = t6.Object_ID AND t1.pos = t6.pos
    LEFT JOIN Subregion t7 ON t1.Object_ID = t7.Object_ID AND t1.pos = t7.pos
    LEFT JOIN Locale t8 ON t1.Object_ID = t8.Object_ID AND t1.pos = t8.pos
    LEFT JOIN Locus t9 ON t1.Object_ID = t9.Object_ID AND t1.pos = t9.pos
    LEFT JOIN Excavation t10 ON t1.Object_ID = t10.Object_ID AND t1.pos = t10.pos    
    LEFT JOIN River t11 ON t1.Object_ID = t11.Object_ID AND t1.pos = t11.pos
ORDER BY t1.Object_ID;


# Drop existing tables 
DROP TABLE Geography_Type, City, State,	County,
	Country, Region, Subregion,	Locale,	Locus,
	Excavation, River;


-- Add Primary to the `geography` Table
ALTER TABLE geography
MODIFY COLUMN Geo_ID BIGINT;

ALTER TABLE geography
ADD CONSTRAINT PK_Geo_ID PRIMARY KEY (Geo_ID);

-- Add Foreign Keys to the `geography` Table
ALTER TABLE geography
ADD CONSTRAINT FK_Artworks_Object_geo
FOREIGN KEY (Object_ID) REFERENCES artworks (Object_ID);


/*******************************************************************************
	GROUP 3
********************************************************************************/
-- Classification
CALL SplitString('Classification', 'Classification', 'Classification');

-- Add Foreign Key to the `Classification` Table
ALTER TABLE Classification
ADD CONSTRAINT FK_Object_class
FOREIGN KEY (Object_ID) REFERENCES artworks (Object_ID);

-- Change `pos` column name
ALTER TABLE Classification
RENAME COLUMN pos TO Classification_Count;

/*******************************************************************************
	GROUP 4
********************************************************************************/
-- Tags
CALL SplitString('Tags', 'Tags', 'Tags');


-- Add Foreign Key to the `Tags` Table
ALTER TABLE Tags
ADD CONSTRAINT FK_Object_tags
FOREIGN KEY (Object_ID) REFERENCES artworks (Object_ID);

-- Change `pos` column name
ALTER TABLE tags
RENAME COLUMN pos TO Tags_Count;



/*******************************************************************************
	Remove columns from `artworks` Table
********************************************************************************/
ALTER TABLE artworks
DROP COLUMN Artist_Display_Name,
DROP COLUMN Artist_Nationality,
DROP COLUMN Artist_Begin_Date,
DROP COLUMN Artist_End_Date,
DROP COLUMN Geography_Type,
DROP COLUMN City,
DROP COLUMN State,
DROP COLUMN County,
DROP COLUMN Country,
DROP COLUMN Region,
DROP COLUMN Subregion,
DROP COLUMN Locale,
DROP COLUMN Locus,
DROP COLUMN Excavation,
DROP COLUMN River,
DROP COLUMN Classification,
DROP COLUMN Tags;



