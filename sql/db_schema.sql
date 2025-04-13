-- Updated schema for options and futures trading data
CREATE TABLE IF NOT EXISTS options_orders (
    id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    strike_price DECIMAL NOT NULL,
    option_type CHAR(2) CHECK (option_type IN ('CE', 'PE')),
    ltp DECIMAL,
    bid_qty INTEGER,
    ask_qty INTEGER,
    lot_size INTEGER,
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (symbol, strike_price, option_type)
);

CREATE TABLE IF NOT EXISTS futures_orders (
    id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    ltp DECIMAL,
    bid_qty INTEGER,
    ask_qty INTEGER,
    lot_size INTEGER,
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (symbol)
);

CREATE TABLE IF NOT EXISTS oi_volume_history (
    id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    expiry_date DATE NOT NULL,
    strike_price DECIMAL NOT NULL,
    option_type CHAR(2) NOT NULL,
    oi DECIMAL,
    volume DECIMAL,
    price DECIMAL,
    display_time VARCHAR(5) NOT NULL, -- HH:MM format
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (symbol, expiry_date, strike_price, option_type, display_time)
);

CREATE TABLE IF NOT EXISTS fno_analytics (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    analytics_type VARCHAR(20) NOT NULL,
    category VARCHAR(20) NOT NULL,
    strike DECIMAL(10,2),
    option_type VARCHAR(5),
    price_change DECIMAL(10,2),
    oi_change DECIMAL(10,2),
    volume_change DECIMAL(10,2),
    absolute_oi INTEGER,
    timestamp TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(symbol, analytics_type, category, strike, option_type, timestamp)
);

CREATE TABLE fiftytwo_week_extremes (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    current_price DECIMAL(12, 2) NOT NULL,
    week52_high DECIMAL(12, 2) NOT NULL,
    week52_low DECIMAL(12, 2) NOT NULL,
    pct_from_high DECIMAL(6, 2) NOT NULL,
    pct_from_low DECIMAL(6, 2) NOT NULL,
    days_since_high INTEGER NOT NULL,
    days_since_low INTEGER NOT NULL,
    status VARCHAR(10) NOT NULL CHECK (status IN ('near_high', 'near_low')),
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Scanner tables
CREATE TABLE IF NOT EXISTS saved_scanners (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    conditions JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    logic VARCHAR(10) NOT NULL CHECK (logic IN ('AND', 'OR')),
    stock_type VARCHAR(10) NOT NULL CHECK (stock_type IN ('fno', 'equity')),
);


CREATE TABLE IF NOT EXISTS stock_data_cache (
                    symbol VARCHAR(20) NOT NULL,
                    interval VARCHAR(5) NOT NULL,
                    timestamp TIMESTAMP NOT NULL,
                    open NUMERIC(10,2),
                    high NUMERIC(10,2),
                    low NUMERIC(10,2),
                    close NUMERIC(10,2),
                    volume NUMERIC(16,2),
                    pivot NUMERIC(10,2),
                    r1 NUMERIC(10,2),
                    r2 NUMERIC(10,2),
                    r3 NUMERIC(10,2),
                    s1 NUMERIC(10,2),
                    s2 NUMERIC(10,2),
                    s3 NUMERIC(10,2),
                    sma20 NUMERIC(10,2),
                    sma50 NUMERIC(10,2),
                    sma200 NUMERIC(10,2),
                    last_updated TIMESTAMP DEFAULT NOW(),
                    PRIMARY KEY (symbol, interval, timestamp)
                );

CREATE INDEX idx_scanner_name ON saved_scanners(name);
CREATE INDEX IF NOT EXISTS idx_stock_data_symbol_interval
                ON stock_data_cache(symbol, interval);

CREATE INDEX IF NOT EXISTS idx_stock_data_interval_timestamp
                ON stock_data_cache(interval, timestamp);

-- Indexes for performance
CREATE INDEX idx_options_symbol_time ON options_orders(symbol, timestamp);
CREATE INDEX idx_futures_symbol_time ON futures_orders(symbol, timestamp);
CREATE INDEX idx_oi_volume_lookup ON oi_volume_history(symbol, expiry_date, strike_price, option_type);
CREATE INDEX idx_oi_volume_time ON oi_volume_history(created_at);

CREATE INDEX idx_52week_symbol ON fiftytwo_week_extremes(symbol);
CREATE INDEX idx_52week_status ON fiftytwo_week_extremes(status);
CREATE INDEX idx_52week_timestamp ON fiftytwo_week_extremes(timestamp);
