-- Connect to the database
\c securities_master;

-- Create schema
CREATE SCHEMA IF NOT EXISTS commodity;

DROP TABLE IF EXISTS commodity.price_data;
DROP TABLE IF EXISTS commodity.technical_indicators;
DROP TABLE IF EXISTS commodity.inventory_data;
DROP TABLE IF EXISTS commodity.rig_count_data;
DROP TABLE IF EXISTS commodity.factors;
DROP TABLE IF EXISTS commodity.features;

-- Price Data Table (WTI, Brent, DXY etc)
CREATE TABLE IF NOT EXISTS commodity.price_data (
    date DATE NOT NULL,
    symbol TEXT NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    PRIMARY KEY (date, symbol)
);

-- Technical Indicators Table (MA, RSI, Bollinger, MACD)
CREATE TABLE IF NOT EXISTS commodity.technical_indicators (
    date DATE NOT NULL,
    symbol TEXT NOT NULL,
    ma50 REAL,
    ma200 REAL,
    bb_upper REAL,
    bb_lower REAL,
    rsi REAL,
    macd REAL,
    macd_signal REAL,
    PRIMARY KEY (date, symbol)
);

-- Inventory Data Table (EIA Crude, Gasoline, Distillate Inventories)
CREATE TABLE IF NOT EXISTS commodity.inventory_data (
    date DATE NOT NULL,
    product TEXT NOT NULL,
    inventory REAL,
    PRIMARY KEY (date, product)
);

-- Rig Count Data Table (Baker Hughes/AOGR Weekly Rig Counts)
CREATE TABLE IF NOT EXISTS commodity.rig_count_data (
    date DATE NOT NULL,
    total_rigs INTEGER,
    oil_rigs INTEGER,
    gas_rigs INTEGER,
    misc_rigs INTEGER,
    weekly_change INTEGER,
    yoy_change INTEGER,
    PRIMARY KEY (date)
);

-- Macro Data Table (DXY & GPR)
CREATE TABLE IF NOT EXISTS commodity.macro_data (
    date DATE NOT NULL,
    dxy REAL,
    gpr REAL,
    PRIMARY KEY (date)
);

-- Factors Table (Derived Quant Factors)
CREATE TABLE IF NOT EXISTS commodity.factors (
    date DATE NOT NULL,
    symbol TEXT NOT NULL,
    inventory_momentum REAL,
    rig_count_momentum REAL,
    macro_pressure REAL,
    PRIMARY KEY (date, symbol)
);

-- Features Table
CREATE TABLE IF NOT EXISTS commodity.features (
    date DATE NOT NULL,
    symbol TEXT NOT NULL,
    feature_name TEXT NOT NULL,
    feature_value DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (date, symbol, feature_name)
);
