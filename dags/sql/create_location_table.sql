DROP TABLE IF EXISTS location;
CREATE TABLE location (
  country VARCHAR(255),
  alpha_2 VARCHAR(2),
  alpha_3 VARCHAR(3),
  country_code INT,
  iso_3166_2 VARCHAR(255),
  region VARCHAR(255),
  sub_region VARCHAR(255),
  intermediate_region VARCHAR(255),
  region_code INT,
  sub_region_code INT,
  intermediate_region_code INT
);
