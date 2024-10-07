use stock_data;
CREATE TABLE fact_table (
    Date DATE,
    Symbol VARCHAR(40),
    Open FLOAT,
    High FLOAT,
    Low FLOAT,
    Close FLOAT
);

SELECT * FROM stock_data.fact_table;