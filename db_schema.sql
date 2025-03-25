-- Tables for all data types
CREATE TABLE IF NOT EXISTS options_data (
    id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    strike_price DECIMAL NOT NULL,
    option_type CHAR(2) CHECK (option_type IN ('CE', 'PE')),
    expiry_date DATE NOT NULL,
    oi BIGINT,
    volume BIGINT,
    last_price DECIMAL,
    timestamp TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (symbol, strike_price, option_type, expiry_date)
);

CREATE TABLE IF NOT EXISTS futures_data (
    id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL UNIQUE,
    ltp DECIMAL,
    bid_qty INTEGER,
    ask_qty INTEGER,
    lot_size INTEGER,
    timestamp TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS oi_volume_data (
    id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    strike_price DECIMAL NOT NULL,
    option_type CHAR(2) NOT NULL,
    expiry_date DATE NOT NULL,
    oi_history JSONB,
    volume_history JSONB,
    price_history JSONB,
    UNIQUE (symbol, strike_price, option_type, expiry_date)
);

-- Indexes for faster queries
CREATE INDEX idx_options_symbol ON options_data(symbol);
CREATE INDEX idx_options_expiry ON options_data(expiry_date);
CREATE INDEX idx_oi_volume_symbol ON oi_volume_data(symbol);
