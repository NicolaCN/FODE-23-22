DROP TABLE IF EXISTS government_measures;
CREATE TABLE government_measures (
    CountryName VARCHAR(255),
    CountryCode VARCHAR(255),
    Jurisdiction VARCHAR(255),
    _date DATE,
    StringencyIndex_Average FLOAT,
    GovernmentResponseIndex_Average FLOAT,
    ContainmentHealthIndex_Average FLOAT,
    EconomicSupportIndex FLOAT,
    PRIMARY KEY (_date, CountryCode)
);
