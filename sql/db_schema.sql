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
    oi DECIMAL,
    volume DECIMAL,
    vega DECIMAL,
    theta DECIMAL,
    gamma DECIMAL,
    delta DECIMAL,
    iv DECIMAL,
    pop DECIMAL,
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
ADD COLUMN IF NOT EXISTS wave_trend NUMERIC(16, 2),
ADD COLUMN IF NOT EXISTS wave_trend_trigger NUMERIC(16, 2),
ADD COLUMN IF NOT EXISTS wave_trend_momentum NUMERIC(16, 2);

ALTER TABLE stock_data_cache
ADD CONSTRAINT unique_symbol_interval UNIQUE (symbol, interval);


-- Drop the existing table
DROP TABLE IF EXISTS scanner_results;

CREATE TABLE scanner_results (
    id SERIAL PRIMARY KEY,
    stock_name VARCHAR(20) NOT NULL,
    scan_date DATE NOT NULL,

    previous_day_breakout BOOLEAN DEFAULT FALSE,
    previous_day_breakdown BOOLEAN DEFAULT FALSE,
    one_week_breakout BOOLEAN DEFAULT FALSE,
    one_week_breakdown BOOLEAN DEFAULT FALSE,
    one_month_breakout BOOLEAN DEFAULT FALSE,
    one_month_breakdown BOOLEAN DEFAULT FALSE,
    fifty_two_week_breakout BOOLEAN DEFAULT FALSE,
    fifty_two_week_breakdown BOOLEAN DEFAULT FALSE,
    all_time_breakout BOOLEAN DEFAULT FALSE,
    all_time_breakdown BOOLEAN DEFAULT FALSE,

    behavior_1_day_opening_high BOOLEAN DEFAULT FALSE,
        behavior_1_day_opening_low BOOLEAN DEFAULT FALSE,
        behavior_2_day_higher_highs BOOLEAN DEFAULT FALSE,
        behavior_2_day_lower_lows BOOLEAN DEFAULT FALSE,
        behavior_3_day_higher_highs BOOLEAN DEFAULT FALSE,
        behavior_3_day_lower_lows BOOLEAN DEFAULT FALSE,

 macd_crossing_signal_line_above BOOLEAN DEFAULT FALSE,
    macd_crossing_signal_line_below BOOLEAN DEFAULT FALSE,
    macd_above_zero BOOLEAN DEFAULT FALSE,
    macd_below_zero BOOLEAN DEFAULT FALSE,
    macd_crossed_signal_line_from_below BOOLEAN DEFAULT FALSE,
    macd_crossed_signal_line_from_above BOOLEAN DEFAULT FALSE,

rsi_bullish BOOLEAN DEFAULT FALSE,
    rsi_trending_up BOOLEAN DEFAULT FALSE,
    rsi_crossed_above_70 BOOLEAN DEFAULT FALSE,
    rsi_bearish BOOLEAN DEFAULT FALSE,
    rsi_trending_down BOOLEAN DEFAULT FALSE,
    rsi_crossed_below_30 BOOLEAN DEFAULT FALSE,
    rsi_overbought_zone BOOLEAN DEFAULT FALSE,
    rsi_oversold_zone BOOLEAN DEFAULT FALSE,

    rsi_weekly_bullish BOOLEAN DEFAULT FALSE,
    rsi_weekly_trending_up BOOLEAN DEFAULT FALSE,
    rsi_weekly_crossed_above_70 BOOLEAN DEFAULT FALSE,
    rsi_weekly_bearish BOOLEAN DEFAULT FALSE,
    rsi_weekly_trending_down BOOLEAN DEFAULT FALSE,
    rsi_weekly_crossed_below_30 BOOLEAN DEFAULT FALSE,
    rsi_weekly_overbought_zone BOOLEAN DEFAULT FALSE,
    rsi_weekly_oversold_zone BOOLEAN DEFAULT FALSE,

adx_crossing_25_from_below BOOLEAN DEFAULT FALSE,
    adx_crossing_25_from_above BOOLEAN DEFAULT FALSE,
    adx_crossing_40_from_below BOOLEAN DEFAULT FALSE,
    adx_crossing_40_from_above BOOLEAN DEFAULT FALSE,
    adx_crossing_10_from_below BOOLEAN DEFAULT FALSE,
    adx_crossing_10_from_above BOOLEAN DEFAULT FALSE,
 plus_di_crossing_minus_di_from_below BOOLEAN DEFAULT FALSE,
    plus_di_crossing_minus_di_from_above BOOLEAN DEFAULT FALSE,
    plus_di_crossing_25_from_below BOOLEAN DEFAULT FALSE,
    minus_di_crossing_25_from_below BOOLEAN DEFAULT FALSE,

close_above_upper_bollinger_band BOOLEAN DEFAULT FALSE,
    close_below_lower_bollinger_band BOOLEAN DEFAULT FALSE,
    close_crossing_upper_bollinger_band_from_below BOOLEAN DEFAULT FALSE,
    close_crossing_lower_bollinger_band_from_above BOOLEAN DEFAULT FALSE,
    close_crossing_upper_bollinger_band_from_above BOOLEAN DEFAULT FALSE,
    close_crossing_lower_bollinger_band_from_below BOOLEAN DEFAULT FALSE,
    bollinger_band_narrowing BOOLEAN DEFAULT FALSE,
    bollinger_band_widening BOOLEAN DEFAULT FALSE,

  atr_3_days_increasing BOOLEAN DEFAULT FALSE,
    atr_3_days_decreasing BOOLEAN DEFAULT FALSE,
    atr_5_days_increasing BOOLEAN DEFAULT FALSE,
    atr_5_days_decreasing BOOLEAN DEFAULT FALSE,
    atr_7_days_increasing BOOLEAN DEFAULT FALSE,
    atr_7_days_decreasing BOOLEAN DEFAULT FALSE,

    momentum_score_1m_increasing BOOLEAN DEFAULT FALSE,
    momentum_score_1m_decreasing BOOLEAN DEFAULT FALSE,
    momentum_score_1m_bullish_zone BOOLEAN DEFAULT FALSE,
    momentum_score_1m_bearish_zone BOOLEAN DEFAULT FALSE,
    momentum_score_3m_increasing BOOLEAN DEFAULT FALSE,
    momentum_score_3m_decreasing BOOLEAN DEFAULT FALSE,
    momentum_score_3m_bullish_zone BOOLEAN DEFAULT FALSE,
    momentum_score_3m_bearish_zone BOOLEAN DEFAULT FALSE,
    momentum_score_6m_increasing BOOLEAN DEFAULT FALSE,
    momentum_score_6m_decreasing BOOLEAN DEFAULT FALSE,
    momentum_score_6m_bullish_zone BOOLEAN DEFAULT FALSE,
    momentum_score_6m_bearish_zone BOOLEAN DEFAULT FALSE,

    candlestick_white_marubozu BOOLEAN DEFAULT FALSE,
    candlestick_black_marubozu BOOLEAN DEFAULT FALSE,
    candlestick_hammer BOOLEAN DEFAULT FALSE,
    candlestick_shooting_star BOOLEAN DEFAULT FALSE,
    candlestick_bullish_engulfing BOOLEAN DEFAULT FALSE,
    candlestick_bearish_engulfing BOOLEAN DEFAULT FALSE,
    candlestick_morning_star BOOLEAN DEFAULT FALSE,
    candlestick_evening_star BOOLEAN DEFAULT FALSE,
    candlestick_dragonfly_doji BOOLEAN DEFAULT FALSE,
        candlestick_inverted_hammer BOOLEAN DEFAULT FALSE,
        candlestick_piercing_line BOOLEAN DEFAULT FALSE,
        candlestick_bullish_harami BOOLEAN DEFAULT FALSE,
        candlestick_bullish_harami_cross BOOLEAN DEFAULT FALSE,
        candlestick_three_white_soldiers BOOLEAN DEFAULT FALSE,
        candlestick_downside_tasuki_gap BOOLEAN DEFAULT FALSE,
        candlestick_falling_three_methods BOOLEAN DEFAULT FALSE,
        candlestick_in_neck BOOLEAN DEFAULT FALSE,
        candlestick_on_neck BOOLEAN DEFAULT FALSE,
        candlestick_thrusting BOOLEAN DEFAULT FALSE,
        candlestick_hanging_man BOOLEAN DEFAULT FALSE,
        candlestick_dark_cloud_cover BOOLEAN DEFAULT FALSE,
        candlestick_bearish_harami BOOLEAN DEFAULT FALSE,
        candlestick_bearish_harami_cross BOOLEAN DEFAULT FALSE,
        candlestick_three_black_crows BOOLEAN DEFAULT FALSE,
        candlestick_doji BOOLEAN DEFAULT FALSE,
        candlestick_near_doji BOOLEAN DEFAULT FALSE,







    srs_ars_above_zero BOOLEAN DEFAULT FALSE,
    srs_ars_below_zero BOOLEAN DEFAULT FALSE,
    srs_crossing_above_zero BOOLEAN DEFAULT FALSE,
    srs_crossing_below_zero BOOLEAN DEFAULT FALSE,
    ars_crossing_above_zero BOOLEAN DEFAULT FALSE,
    ars_crossing_below_zero BOOLEAN DEFAULT FALSE,
    both_above_zero_and_ars_rising BOOLEAN DEFAULT FALSE,
    both_above_zero_and_srs_rising BOOLEAN DEFAULT FALSE,


       close_near_sma_20 BOOLEAN DEFAULT FALSE,
       close_crossing_sma_20_above BOOLEAN DEFAULT FALSE,
       close_crossing_sma_20_below BOOLEAN DEFAULT FALSE,
       close_near_sma_50 BOOLEAN DEFAULT FALSE,
       close_crossing_sma_50_above BOOLEAN DEFAULT FALSE,
       close_crossing_sma_50_below BOOLEAN DEFAULT FALSE,
       close_near_sma_100 BOOLEAN DEFAULT FALSE,
       close_crossing_sma_100_above BOOLEAN DEFAULT FALSE,
       close_crossing_sma_100_below BOOLEAN DEFAULT FALSE,
       close_near_sma_200 BOOLEAN DEFAULT FALSE,
       close_crossing_sma_200_above BOOLEAN DEFAULT FALSE,
       close_crossing_sma_200_below BOOLEAN DEFAULT FALSE,

       sma_5_crossing_sma_20_above BOOLEAN DEFAULT FALSE,
       sma_5_crossing_sma_20_below BOOLEAN DEFAULT FALSE,
       sma_20_crossing_sma_50_above BOOLEAN DEFAULT FALSE,
       sma_20_crossing_sma_50_below BOOLEAN DEFAULT FALSE,
       sma_20_crossing_sma_100_above BOOLEAN DEFAULT FALSE,
       sma_20_crossing_sma_100_below BOOLEAN DEFAULT FALSE,
       sma_20_crossing_sma_200_above BOOLEAN DEFAULT FALSE,
       sma_20_crossing_sma_200_below BOOLEAN DEFAULT FALSE,
       sma_50_crossing_sma_100_above BOOLEAN DEFAULT FALSE,
       sma_50_crossing_sma_100_below BOOLEAN DEFAULT FALSE,
       sma_50_crossing_sma_200_above BOOLEAN DEFAULT FALSE,
       sma_50_crossing_sma_200_below BOOLEAN DEFAULT FALSE,
       sma_100_crossing_sma_200_above BOOLEAN DEFAULT FALSE,
       sma_100_crossing_sma_200_below BOOLEAN DEFAULT FALSE,

       close_near_weekly_sma_20 BOOLEAN DEFAULT FALSE,
       close_crossing_weekly_sma_20_above BOOLEAN DEFAULT FALSE,
       close_crossing_weekly_sma_20_below BOOLEAN DEFAULT FALSE,
       close_near_weekly_sma_50 BOOLEAN DEFAULT FALSE,
       close_crossing_weekly_sma_50_above BOOLEAN DEFAULT FALSE,
       close_crossing_weekly_sma_50_below BOOLEAN DEFAULT FALSE,
       close_near_weekly_sma_100 BOOLEAN DEFAULT FALSE,
       close_crossing_weekly_sma_100_above BOOLEAN DEFAULT FALSE,
       close_crossing_weekly_sma_100_below BOOLEAN DEFAULT FALSE,
       close_near_weekly_sma_200 BOOLEAN DEFAULT FALSE,
       close_crossing_weekly_sma_200_above BOOLEAN DEFAULT FALSE,
       close_crossing_weekly_sma_200_below BOOLEAN DEFAULT FALSE,

       sma_5_crossing_weekly_sma_20_above BOOLEAN DEFAULT FALSE,
       sma_5_crossing_weekly_sma_20_below BOOLEAN DEFAULT FALSE,
       sma_20_crossing_weekly_sma_50_above BOOLEAN DEFAULT FALSE,
       sma_20_crossing_weekly_sma_50_below BOOLEAN DEFAULT FALSE,
       sma_20_crossing_weekly_sma_100_above BOOLEAN DEFAULT FALSE,
       sma_20_crossing_weekly_sma_100_below BOOLEAN DEFAULT FALSE,
       sma_20_crossing_weekly_sma_200_above BOOLEAN DEFAULT FALSE,
       sma_20_crossing_weekly_sma_200_below BOOLEAN DEFAULT FALSE,
       sma_50_crossing_weekly_sma_100_above BOOLEAN DEFAULT FALSE,
       sma_50_crossing_weekly_sma_100_below BOOLEAN DEFAULT FALSE,
       sma_50_crossing_weekly_sma_200_above BOOLEAN DEFAULT FALSE,
       sma_50_crossing_weekly_sma_200_below BOOLEAN DEFAULT FALSE,
       sma_100_crossing_weekly_sma_200_above BOOLEAN DEFAULT FALSE,
       sma_100_crossing_weekly_sma_200_below BOOLEAN DEFAULT FALSE,

       close_near_ema_20 BOOLEAN DEFAULT FALSE,
       close_crossing_ema_20_above BOOLEAN DEFAULT FALSE,
       close_crossing_ema_20_below BOOLEAN DEFAULT FALSE,
       close_near_ema_50 BOOLEAN DEFAULT FALSE,
       close_crossing_ema_50_above BOOLEAN DEFAULT FALSE,
       close_crossing_ema_50_below BOOLEAN DEFAULT FALSE,
       close_near_ema_100 BOOLEAN DEFAULT FALSE,
       close_crossing_ema_100_above BOOLEAN DEFAULT FALSE,
       close_crossing_ema_100_below BOOLEAN DEFAULT FALSE,
       close_near_ema_200 BOOLEAN DEFAULT FALSE,
       close_crossing_ema_200_above BOOLEAN DEFAULT FALSE,
       close_crossing_ema_200_below BOOLEAN DEFAULT FALSE,

       ema_5_crossing_ema_20_above BOOLEAN DEFAULT FALSE,
       ema_5_crossing_ema_20_below BOOLEAN DEFAULT FALSE,
       ema_20_crossing_ema_50_above BOOLEAN DEFAULT FALSE,
       ema_20_crossing_ema_50_below BOOLEAN DEFAULT FALSE,
       ema_20_crossing_ema_100_above BOOLEAN DEFAULT FALSE,
       ema_20_crossing_ema_100_below BOOLEAN DEFAULT FALSE,
       ema_20_crossing_ema_200_above BOOLEAN DEFAULT FALSE,
       ema_20_crossing_ema_200_below BOOLEAN DEFAULT FALSE,
       ema_50_crossing_ema_100_above BOOLEAN DEFAULT FALSE,
       ema_50_crossing_ema_100_below BOOLEAN DEFAULT FALSE,
       ema_50_crossing_ema_200_above BOOLEAN DEFAULT FALSE,
       ema_50_crossing_ema_200_below BOOLEAN DEFAULT FALSE,
       ema_100_crossing_ema_200_above BOOLEAN DEFAULT FALSE,
       ema_100_crossing_ema_200_below BOOLEAN DEFAULT FALSE,

       close_near_weekly_ema_20 BOOLEAN DEFAULT FALSE,
       close_crossing_weekly_ema_20_above BOOLEAN DEFAULT FALSE,
       close_crossing_weekly_ema_20_below BOOLEAN DEFAULT FALSE,
       close_near_weekly_ema_50 BOOLEAN DEFAULT FALSE,
       close_crossing_weekly_ema_50_above BOOLEAN DEFAULT FALSE,
       close_crossing_weekly_ema_50_below BOOLEAN DEFAULT FALSE,
       close_near_weekly_ema_100 BOOLEAN DEFAULT FALSE,
       close_crossing_weekly_ema_100_above BOOLEAN DEFAULT FALSE,
       close_crossing_weekly_ema_100_below BOOLEAN DEFAULT FALSE,
       close_near_weekly_ema_200 BOOLEAN DEFAULT FALSE,
       close_crossing_weekly_ema_200_above BOOLEAN DEFAULT FALSE,
       close_crossing_weekly_ema_200_below BOOLEAN DEFAULT FALSE,

       ema_5_crossing_weekly_ema_20_above BOOLEAN DEFAULT FALSE,
       ema_5_crossing_weekly_ema_20_below BOOLEAN DEFAULT FALSE,
       ema_20_crossing_weekly_ema_50_above BOOLEAN DEFAULT FALSE,
       ema_20_crossing_weekly_ema_50_below BOOLEAN DEFAULT FALSE,
       ema_20_crossing_weekly_ema_100_above BOOLEAN DEFAULT FALSE,
       ema_20_crossing_weekly_ema_100_below BOOLEAN DEFAULT FALSE,
       ema_20_crossing_weekly_ema_200_above BOOLEAN DEFAULT FALSE,
       ema_20_crossing_weekly_ema_200_below BOOLEAN DEFAULT FALSE,
       ema_50_crossing_weekly_ema_100_above BOOLEAN DEFAULT FALSE,
       ema_50_crossing_weekly_ema_100_below BOOLEAN DEFAULT FALSE,
       ema_50_crossing_weekly_ema_200_above BOOLEAN DEFAULT FALSE,
       ema_50_crossing_weekly_ema_200_below BOOLEAN DEFAULT FALSE,
       ema_100_crossing_weekly_ema_200_above BOOLEAN DEFAULT FALSE,
       ema_100_crossing_weekly_ema_200_below BOOLEAN DEFAULT FALSE,

weekly_supertrend_signal_changed_to_buy BOOLEAN DEFAULT FALSE,
    weekly_supertrend_signal_changed_to_sell BOOLEAN DEFAULT FALSE,
    price_nearing_weekly_supertrend_support BOOLEAN DEFAULT FALSE,
    price_nearing_weekly_supertrend_resistance BOOLEAN DEFAULT FALSE,

    nr4 BOOLEAN DEFAULT FALSE,
    nr7 BOOLEAN DEFAULT FALSE,
    nr4_breakout BOOLEAN DEFAULT FALSE,
    nr4_breakdown BOOLEAN DEFAULT FALSE,
    nr7_breakout BOOLEAN DEFAULT FALSE,
    nr7_breakdown BOOLEAN DEFAULT FALSE,

    supertrend_signal_changed_to_buy BOOLEAN DEFAULT FALSE,
    supertrend_signal_changed_to_sell BOOLEAN DEFAULT FALSE,
    price_nearing_supertrend_support BOOLEAN DEFAULT FALSE,
    price_nearing_supertrend_resistance BOOLEAN DEFAULT FALSE,

    absolute_return_1_month BOOLEAN DEFAULT FALSE,
    absolute_return_3_months BOOLEAN DEFAULT FALSE,
    absolute_return_6_months BOOLEAN DEFAULT FALSE,
    absolute_return_1_year BOOLEAN DEFAULT FALSE,





    cci_bullish BOOLEAN DEFAULT FALSE,
    cci_trending_up BOOLEAN DEFAULT FALSE,
    cci_crossed_above_100 BOOLEAN DEFAULT FALSE,
    cci_bearish BOOLEAN DEFAULT FALSE,
    cci_trending_down BOOLEAN DEFAULT FALSE,
    cci_crossed_below_100 BOOLEAN DEFAULT FALSE,
    cci_overbought_zone BOOLEAN DEFAULT FALSE,
    cci_oversold_zone BOOLEAN DEFAULT FALSE,

    mfi_bullish BOOLEAN DEFAULT FALSE,
    mfi_trending_up BOOLEAN DEFAULT FALSE,
    mfi_crossed_80_from_below BOOLEAN DEFAULT FALSE,
    mfi_bearish BOOLEAN DEFAULT FALSE,
    mfi_trending_down BOOLEAN DEFAULT FALSE,
    mfi_crossed_20_from_above BOOLEAN DEFAULT FALSE,
    mfi_above_80 BOOLEAN DEFAULT FALSE,
    mfi_below_20 BOOLEAN DEFAULT FALSE,

    williams_r_bullish BOOLEAN DEFAULT FALSE,
    williams_r_trending_up BOOLEAN DEFAULT FALSE,
    williams_r_crossed_20_from_below BOOLEAN DEFAULT FALSE,
    williams_r_bearish BOOLEAN DEFAULT FALSE,
    williams_r_trending_down BOOLEAN DEFAULT FALSE,
    williams_r_crossed_80_from_above BOOLEAN DEFAULT FALSE,
    williams_r_above_20 BOOLEAN DEFAULT FALSE,
    williams_r_below_80 BOOLEAN DEFAULT FALSE,

    roc_trending_up BOOLEAN DEFAULT FALSE,
    roc_trending_down BOOLEAN DEFAULT FALSE,
    roc_increasing BOOLEAN DEFAULT FALSE,
    roc_decreasing BOOLEAN DEFAULT FALSE,


    stochastic_in_overbought_zone BOOLEAN DEFAULT FALSE,
    stochastic_in_oversold_zone BOOLEAN DEFAULT FALSE,
    stochastic_entering_overbought_zone BOOLEAN DEFAULT FALSE,
    stochastic_entering_oversold_zone BOOLEAN DEFAULT FALSE,
    stochastic_trending_up BOOLEAN DEFAULT FALSE,
    stochastic_trending_down BOOLEAN DEFAULT FALSE,
    stochastic_k_crossing_d_from_below BOOLEAN DEFAULT FALSE,
    stochastic_k_crossing_d_from_above BOOLEAN DEFAULT FALSE,

    psar_bullish_reversal BOOLEAN DEFAULT FALSE,
    psar_bearish_reversal BOOLEAN DEFAULT FALSE,
    psar_weekly_bullish_reversal BOOLEAN DEFAULT FALSE,
    psar_weekly_bearish_reversal BOOLEAN DEFAULT FALSE,

    -- New futures OI scan fields
    futures_high_oi_increase BOOLEAN DEFAULT FALSE,
    futures_high_oi_decrease BOOLEAN DEFAULT FALSE,
    
    -- New option OI scan fields
    call_options_high_oi_increase BOOLEAN DEFAULT FALSE,
    call_options_high_oi_decrease BOOLEAN DEFAULT FALSE,
    put_options_high_oi_increase BOOLEAN DEFAULT FALSE,
    put_options_high_oi_decrease BOOLEAN DEFAULT FALSE,

    UNIQUE (stock_name, scan_date)
);

-- Add the double top and double bottom pattern columns to the scanner_results table
ALTER TABLE scanner_results
ADD COLUMN pattern_double_top BOOLEAN DEFAULT FALSE,
ADD COLUMN pattern_double_bottom BOOLEAN DEFAULT FALSE,
ADD COLUMN pattern_head_and_shoulders BOOLEAN DEFAULT FALSE,
ADD COLUMN pattern_inverse_head_and_shoulders BOOLEAN DEFAULT FALSE,
ADD COLUMN pattern_bullish_engulfing BOOLEAN DEFAULT FALSE,
ADD COLUMN pattern_bearish_engulfing BOOLEAN DEFAULT FALSE,
ADD COLUMN pattern_hammer BOOLEAN DEFAULT FALSE,
ADD COLUMN pattern_shooting_star BOOLEAN DEFAULT FALSE,
ADD COLUMN pattern_rising_wedge BOOLEAN DEFAULT FALSE,
ADD COLUMN pattern_falling_wedge BOOLEAN DEFAULT FALSE,
ADD COLUMN pattern_triangle_bullish BOOLEAN DEFAULT FALSE,
ADD COLUMN pattern_triangle_bearish BOOLEAN DEFAULT FALSE,
ADD COLUMN pattern_pennant_bullish BOOLEAN DEFAULT FALSE,
ADD COLUMN pattern_pennant_bearish BOOLEAN DEFAULT FALSE,
ADD COLUMN pattern_rectangle_bullish BOOLEAN DEFAULT FALSE,
ADD COLUMN pattern_rectangle_bearish BOOLEAN DEFAULT FALSE,
ADD COLUMN pattern_flag_bullish BOOLEAN DEFAULT FALSE,
ADD COLUMN pattern_flag_bearish BOOLEAN DEFAULT FALSE,
ADD COLUMN pattern_channel_rising BOOLEAN DEFAULT FALSE,
ADD COLUMN pattern_channel_falling BOOLEAN DEFAULT FALSE,
ADD COLUMN pattern_channel_horizontal BOOLEAN DEFAULT FALSE,
ADD COLUMN pattern_triple_top BOOLEAN DEFAULT FALSE,
ADD COLUMN pattern_triple_bottom BOOLEAN DEFAULT FALSE,
ADD COLUMN pattern_cup_and_handle BOOLEAN DEFAULT FALSE,
ADD COLUMN pattern_rounding_bottom BOOLEAN DEFAULT FALSE,
ADD COLUMN pattern_diamond_bullish BOOLEAN DEFAULT FALSE,
ADD COLUMN pattern_diamond_bearish BOOLEAN DEFAULT FALSE;

CREATE INDEX idx_scanner_results_stock_date ON scanner_results (stock_name, scan_date);
CREATE INDEX idx_scanner_results_date ON scanner_results (scan_date);

CREATE TABLE IF NOT EXISTS quarterly_financials (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    quarter_ending DATE NOT NULL,
    revenue NUMERIC(16, 2),
    net_income NUMERIC(16, 2),
    operating_income NUMERIC(16, 2),
    ebitda NUMERIC(16, 2),
    type VARCHAR(10) NOT NULL CHECK (type IN ('current', 'previous')),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (symbol, quarter_ending)
);

CREATE INDEX idx_quarterly_financials_symbol ON quarterly_financials(symbol);
CREATE INDEX idx_quarterly_financials_date ON quarterly_financials(quarter_ending);

CREATE TABLE IF NOT EXISTS stock_earnings (
    symbol VARCHAR(20) PRIMARY KEY,
    next_earnings_date TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_stock_earnings_date ON stock_earnings(next_earnings_date);

ALTER TABLE stock_data_cache
        ADD COLUMN week52_high DECIMAL(20, 4),
        ADD COLUMN week52_low DECIMAL(20, 4),
        ADD COLUMN pct_from_week52_high DECIMAL(10, 2),
        ADD COLUMN pct_from_week52_low DECIMAL(10, 2),
        ADD COLUMN days_since_week52_high INTEGER,
        ADD COLUMN days_since_week52_low INTEGER,
        ADD COLUMN week52_status VARCHAR(20);

ALTER TABLE oi_volume_history
ADD COLUMN IF NOT EXISTS vega DECIMAL,
ADD COLUMN IF NOT EXISTS theta DECIMAL,
ADD COLUMN IF NOT EXISTS gamma DECIMAL,
ADD COLUMN IF NOT EXISTS delta DECIMAL,
ADD COLUMN IF NOT EXISTS iv DECIMAL,
ADD COLUMN IF NOT EXISTS pop DECIMAL;

-- Create table for instrument keys
CREATE TABLE IF NOT EXISTS instrument_keys (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    instrument_key VARCHAR(100) NOT NULL,
    exchange VARCHAR(20) NOT NULL,
    tradingsymbol VARCHAR(100) NOT NULL,
    lot_size INTEGER,
    instrument_type VARCHAR(20) NOT NULL,
    expiry_date DATE,
    strike_price DECIMAL(20, 2),
    option_type VARCHAR(5),
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE (tradingsymbol, exchange)
);

ALTER TABLE instrument_keys
ADD COLUMN IF NOT EXISTS prev_close DECIMAL(20, 4);

-- Create indexes for efficient queries
CREATE INDEX IF NOT EXISTS idx_instrument_keys_symbol ON instrument_keys(symbol);
CREATE INDEX IF NOT EXISTS idx_instrument_keys_instrument_key ON instrument_keys(instrument_key);
CREATE INDEX IF NOT EXISTS idx_instrument_keys_instrument_type ON instrument_keys(instrument_type);
CREATE INDEX IF NOT EXISTS idx_instrument_keys_expiry_date ON instrument_keys(expiry_date);

CREATE TABLE IF NOT EXISTS upstox_accounts (
    id SERIAL PRIMARY KEY,
    api_key VARCHAR(255) NOT NULL UNIQUE,
    api_secret VARCHAR(255) NOT NULL,
    totp_secret VARCHAR(255) NOT NULL,
    redirect_uri VARCHAR(255) NOT NULL,
    username VARCHAR(255) NOT NULL,
    password VARCHAR(255) NOT NULL,
    access_token TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Add index for performance
CREATE INDEX IF NOT EXISTS idx_upstox_accounts_api_key ON upstox_accounts(api_key);

-- Add option greeks, OI and volume columns to options_orders table if they don't exist
ALTER TABLE options_orders
ADD COLUMN IF NOT EXISTS oi DECIMAL,
ADD COLUMN IF NOT EXISTS volume DECIMAL,
ADD COLUMN IF NOT EXISTS vega DECIMAL,
ADD COLUMN IF NOT EXISTS theta DECIMAL,
ADD COLUMN IF NOT EXISTS gamma DECIMAL,
ADD COLUMN IF NOT EXISTS delta DECIMAL,
ADD COLUMN IF NOT EXISTS iv DECIMAL,
ADD COLUMN IF NOT EXISTS pop DECIMAL;

-- Ensure timestamp is stored in IST timezone
ALTER TABLE options_orders
ALTER COLUMN timestamp TYPE TIMESTAMPTZ;

ALTER TABLE futures_orders
ALTER COLUMN timestamp TYPE TIMESTAMPTZ;

