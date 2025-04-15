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

ALTER TABLE stock_data_cache
ADD COLUMN IF NOT EXISTS ichimoku_base NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS ichimoku_conversion NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS ichimoku_span_a NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS ichimoku_span_b NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS ichimoku_cloud_top NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS ichimoku_cloud_bottom NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS stoch_rsi NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS parabolic_sar NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS upper_bollinger NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS lower_bollinger NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS macd_line NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS macd_signal NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS macd_histogram NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS adx NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS adx_di_positive NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS adx_di_negative NUMERIC(10, 2);

ALTER TABLE stock_data_cache
ADD COLUMN IF NOT EXISTS ha_open NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS ha_close NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS ha_high NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS ha_low NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS supertrend NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS acc_dist NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS cci NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS cmf NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS mfi NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS on_balance_volume NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS williams_r NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS bollinger_b NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS intraday_intensity NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS force_index NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS stc NUMERIC(10, 2);

ALTER TABLE stock_data_cache
ADD COLUMN IF NOT EXISTS sma10 NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS sma50 NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS sma100 NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS sma200 NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS ema10 NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS ema50 NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS ema100 NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS ema200 NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS wma10 NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS wma50 NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS wma100 NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS wma200 NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS tma10 NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS tma50 NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS tma100 NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS tma200 NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS rma10 NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS rma50 NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS rma100 NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS rma200 NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS tema10 NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS tema50 NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS tema100 NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS tema200 NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS hma10 NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS hma50 NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS hma100 NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS hma200 NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS vwma10 NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS vwma50 NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS vwma100 NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS vwma200 NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS std10 NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS std50 NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS std100 NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS std200 NUMERIC(10, 2);

ALTER TABLE stock_data_cache
ADD COLUMN IF NOT EXISTS ATR NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS TRIX NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS ROC NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS Keltner_Middle NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS Keltner_Upper NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS Keltner_Lower NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS Donchian_High NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS Donchian_Low NUMERIC(10, 2),
ADD COLUMN IF NOT EXISTS Chaikin_Oscillator NUMERIC(10, 2);

-- Add middle_bollinger column for complete Bollinger Bands implementation
ALTER TABLE stock_data_cache
ADD COLUMN IF NOT EXISTS middle_bollinger NUMERIC(16, 2);

-- Add missing column to stock_data_cache table if it doesn't exist
ALTER TABLE stock_data_cache
ADD COLUMN IF NOT EXISTS rsi NUMERIC(16, 2);

-- Ensure all required columns for indicators exist
ALTER TABLE stock_data_cache
ADD COLUMN IF NOT EXISTS sma20 NUMERIC(16, 2);

-- Make sure columns added have the correct type
ALTER TABLE stock_data_cache
    ALTER COLUMN rsi TYPE NUMERIC(16, 2);

ALTER TABLE stock_data_cache
    ALTER COLUMN open TYPE NUMERIC(16, 2),
    ALTER COLUMN high TYPE NUMERIC(16, 2),
    ALTER COLUMN low TYPE NUMERIC(16, 2),
    ALTER COLUMN close TYPE NUMERIC(16, 2),
    ALTER COLUMN volume TYPE NUMERIC(20, 2),
    ALTER COLUMN pivot TYPE NUMERIC(16, 2),
    ALTER COLUMN r1 TYPE NUMERIC(16, 2),
    ALTER COLUMN r2 TYPE NUMERIC(16, 2),
    ALTER COLUMN r3 TYPE NUMERIC(16, 2),
    ALTER COLUMN s1 TYPE NUMERIC(16, 2),
    ALTER COLUMN s2 TYPE NUMERIC(16, 2),
    ALTER COLUMN s3 TYPE NUMERIC(16, 2),
    ALTER COLUMN sma20 TYPE NUMERIC(16, 2),
    ALTER COLUMN sma50 TYPE NUMERIC(16, 2),
    ALTER COLUMN sma200 TYPE NUMERIC(16, 2),
    ALTER COLUMN ichimoku_base TYPE NUMERIC(16, 2),
    ALTER COLUMN ichimoku_conversion TYPE NUMERIC(16, 2),
    ALTER COLUMN ichimoku_span_a TYPE NUMERIC(16, 2),
    ALTER COLUMN ichimoku_span_b TYPE NUMERIC(16, 2),
    ALTER COLUMN ichimoku_cloud_top TYPE NUMERIC(16, 2),
    ALTER COLUMN ichimoku_cloud_bottom TYPE NUMERIC(16, 2),
    ALTER COLUMN stoch_rsi TYPE NUMERIC(16, 2),
    ALTER COLUMN parabolic_sar TYPE NUMERIC(16, 2),
    ALTER COLUMN upper_bollinger TYPE NUMERIC(16, 2),
    ALTER COLUMN lower_bollinger TYPE NUMERIC(16, 2),
    ALTER COLUMN macd_line TYPE NUMERIC(16, 2),
    ALTER COLUMN macd_signal TYPE NUMERIC(16, 2),
    ALTER COLUMN macd_histogram TYPE NUMERIC(16, 2),
    ALTER COLUMN adx TYPE NUMERIC(16, 2),
    ALTER COLUMN adx_di_positive TYPE NUMERIC(16, 2),
    ALTER COLUMN adx_di_negative TYPE NUMERIC(16, 2),
    ALTER COLUMN ha_open TYPE NUMERIC(16, 2),
    ALTER COLUMN ha_close TYPE NUMERIC(16, 2),
    ALTER COLUMN ha_high TYPE NUMERIC(16, 2),
    ALTER COLUMN ha_low TYPE NUMERIC(16, 2),
    ALTER COLUMN supertrend TYPE NUMERIC(16, 2),
    ALTER COLUMN acc_dist TYPE NUMERIC(16, 2),
    ALTER COLUMN cci TYPE NUMERIC(16, 2),
    ALTER COLUMN cmf TYPE NUMERIC(16, 2),
    ALTER COLUMN mfi TYPE NUMERIC(16, 2),
    ALTER COLUMN on_balance_volume TYPE NUMERIC(16, 2),
    ALTER COLUMN williams_r TYPE NUMERIC(16, 2),
    ALTER COLUMN bollinger_b TYPE NUMERIC(16, 2),
    ALTER COLUMN intraday_intensity TYPE NUMERIC(16, 2),
    ALTER COLUMN force_index TYPE NUMERIC(16, 2),
    ALTER COLUMN stc TYPE NUMERIC(16, 2),
    ALTER COLUMN ATR TYPE NUMERIC(16, 2),
    ALTER COLUMN TRIX TYPE NUMERIC(16, 2),
    ALTER COLUMN ROC TYPE NUMERIC(16, 2),
    ALTER COLUMN Keltner_Middle TYPE NUMERIC(16, 2),
    ALTER COLUMN Keltner_Upper TYPE NUMERIC(16, 2),
    ALTER COLUMN Keltner_Lower TYPE NUMERIC(16, 2),
    ALTER COLUMN Donchian_High TYPE NUMERIC(16, 2),
    ALTER COLUMN Donchian_Low TYPE NUMERIC(16, 2),
    ALTER COLUMN Chaikin_Oscillator TYPE NUMERIC(16, 2),
    ALTER COLUMN middle_bollinger TYPE NUMERIC(16, 2);

