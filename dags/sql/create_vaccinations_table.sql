DROP TABLE IF EXISTS vaccinations;
CREATE TABLE vaccinations (
    location VARCHAR,
    iso_code VARCHAR,
    date DATE,
    total_vaccinations INTEGER,
    people_vaccinated INTEGER,
    people_fully_vaccinated INTEGER,
    total_boosters INTEGER,
    daily_vaccinations_raw INTEGER,
    daily_vaccinations INTEGER,
    total_vaccinations_per_hundred DECIMAL,
    people_vaccinated_per_hundred DECIMAL,
    people_fully_vaccinated_per_hundred DECIMAL,
    total_boosters_per_hundred DECIMAL,
    daily_vaccinations_per_million DECIMAL,
    daily_people_vaccinated INTEGER,
    daily_people_vaccinated_per_hundred DECIMAL,
    PRIMARY KEY (date, iso_code)
);
