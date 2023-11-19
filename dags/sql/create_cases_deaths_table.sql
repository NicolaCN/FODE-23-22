
DROP TABLE IF EXISTS cases_deaths;
CREATE TABLE IF NOT EXISTS cases_deaths (
    Date_reported DATE,
    Country VARCHAR(255),
    Country_code VARCHAR(255),
    WHO_region VARCHAR(255),
    New_cases INTEGER,
    Cumulative_cases INTEGER,
    New_deaths INTEGER,
    Cumulative_deaths INTEGER,
    Weekly_cases INTEGER,
    Weekly_deaths INTEGER,
    Weekly_pct_growth_cases FLOAT,
    Weekly_pct_growth_deaths FLOAT,
    population INTEGER,
    New_cases_per_million FLOAT,
    New_deaths_per_million FLOAT,
    Cumulative_cases_per_million FLOAT,
    Cumulative_deaths_per_million FLOAT,
    Weekly_cases_per_million FLOAT,
    Weekly_deaths_per_million FLOAT,
    PRIMARY KEY (Date_reported, Country_code)
);
