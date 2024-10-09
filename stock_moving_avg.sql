use stock_data;
CREATE TABLE IF NOT EXISTS stock_moving_avg (
    Date DATE,
    Symbol VARCHAR(10),
    Close FLOAT,
    moving_avg_7 FLOAT,
    moving_avg_14 FLOAT,
    moving_avg_21 FLOAT,
    moving_avg_28 FLOAT
)