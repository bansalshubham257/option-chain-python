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

-- Indexes for performance
CREATE INDEX idx_options_symbol_time ON options_orders(symbol, timestamp);
CREATE INDEX idx_futures_symbol_time ON futures_orders(symbol, timestamp);
CREATE INDEX idx_oi_volume_lookup ON oi_volume_history(symbol, expiry_date, strike_price, option_type);
CREATE INDEX idx_oi_volume_time ON oi_volume_history(created_at);

CREATE INDEX idx_52week_symbol ON fiftytwo_week_extremes(symbol);
CREATE INDEX idx_52week_status ON fiftytwo_week_extremes(status);
CREATE INDEX idx_52week_timestamp ON fiftytwo_week_extremes(timestamp);
