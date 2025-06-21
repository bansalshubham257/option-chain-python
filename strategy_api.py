import json
from datetime import datetime, date
from fastapi import FastAPI, Query, Path, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from typing import Dict, List, Optional, Any
import pandas as pd
import logging

from services.database import DatabaseService

# Configure logging - only console output, no log files
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # Only log to console, no file logging
    ]
)
logger = logging.getLogger("strategy_api")

# Initialize FastAPI app
app = FastAPI(title="Strategy API", description="API for strategy performance tracking")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize database service
db_service = DatabaseService()

# List of available strategies
STRATEGIES = ["percent_change_tracker", "aggressive_tracker"]

# Helper function to format decimal values for JSON response
def format_decimal(value):
    if value is None:
        return 0
    return float(value)

@app.get("/api/strategies", tags=["Strategies"])
async def get_available_strategies():
    """Get list of available strategies"""
    return {
        "strategies": [
            {
                "name": "percent_change_tracker",
                "display_name": "Conservative (5% Entry, 15% SL)",
                "description": "Enter on 5% price change, 95% profit target, 15% stop loss"
            },
            {
                "name": "aggressive_tracker",
                "display_name": "Aggressive (10% Entry, 30% SL)",
                "description": "Enter on 10% price change, 95% profit target, 30% stop loss"
            }
        ]
    }

@app.get("/api/strategy/active-orders", tags=["Strategy"])
async def get_active_orders(strategy: str = Query(None, description="Strategy name")):
    """Get active (open) orders for a strategy"""
    try:
        with db_service._get_cursor() as cur:
            query = """
                SELECT 
                    id, symbol, strike_price, option_type, 
                    entry_price, current_price, target_price, stop_loss, 
                    quantity, lot_size, entry_time, strategy_name,
                    expiry_date, instrument_key
                FROM strategy_orders
                WHERE status = 'OPEN'
            """

            params = []
            if strategy:
                query += " AND strategy_name = %s"
                params.append(strategy)

            query += " ORDER BY entry_time DESC"

            cur.execute(query, params)
            rows = cur.fetchall()

            orders = []
            for row in rows:
                # Calculate percent change (unrealized P&L)
                entry_price = float(row[4])
                current_price = float(row[5] or entry_price)
                pnl_percent = ((current_price - entry_price) / entry_price) * 100

                orders.append({
                    "id": row[0],
                    "symbol": row[1],
                    "strike_price": row[2],
                    "option_type": row[3],
                    "entry_price": format_decimal(row[4]),
                    "current_price": format_decimal(row[5]),
                    "target_price": format_decimal(row[6]),
                    "stop_loss": format_decimal(row[7]),
                    "quantity": row[8],
                    "lot_size": row[9],
                    "entry_time": row[10].isoformat() if row[10] else None,
                    "strategy_name": row[11],
                    "expiry_date": row[12].isoformat() if row[12] else None,
                    "instrument_key": row[13],
                    "unrealized_pnl": format_decimal((current_price - entry_price) * row[8]),
                    "pnl_percentage": round(pnl_percent, 2)
                })

            return {"orders": orders}

    except Exception as e:
        logger.error(f"Error getting active orders: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/strategy/closed-orders", tags=["Strategy"])
async def get_closed_orders(
    strategy: str = Query(None, description="Strategy name"),
    limit: int = Query(100, description="Maximum number of orders to return"),
    status: str = Query(None, description="Filter by status (PROFIT, LOSS, EXPIRED)")
):
    """Get closed orders for a strategy"""
    try:
        with db_service._get_cursor() as cur:
            query = """
                SELECT 
                    id, symbol, strike_price, option_type, 
                    entry_price, exit_price, quantity, lot_size, 
                    entry_time, exit_time, status, pnl, 
                    pnl_percentage, strategy_name
                FROM strategy_orders
                WHERE status != 'OPEN'
            """

            params = []
            if strategy:
                query += " AND strategy_name = %s"
                params.append(strategy)

            if status:
                query += " AND status = %s"
                params.append(status)

            query += " ORDER BY exit_time DESC LIMIT %s"
            params.append(limit)

            cur.execute(query, params)
            rows = cur.fetchall()

            orders = []
            for row in rows:
                orders.append({
                    "id": row[0],
                    "symbol": row[1],
                    "strike_price": row[2],
                    "option_type": row[3],
                    "entry_price": format_decimal(row[4]),
                    "exit_price": format_decimal(row[5]),
                    "quantity": row[6],
                    "lot_size": row[7],
                    "entry_time": row[8].isoformat() if row[8] else None,
                    "exit_time": row[9].isoformat() if row[9] else None,
                    "status": row[10],
                    "pnl": format_decimal(row[11]),
                    "pnl_percentage": format_decimal(row[12]),
                    "strategy_name": row[13]
                })

            return {"orders": orders}

    except Exception as e:
        logger.error(f"Error getting closed orders: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/strategy/monthly-performance", tags=["Strategy"])
async def get_monthly_performance(strategy: str = Query(None, description="Strategy name")):
    """Get monthly performance for a strategy"""
    try:
        with db_service._get_cursor() as cur:
            query = """
                SELECT 
                    month, year, strategy_name, total_orders, 
                    profit_orders, loss_orders, expired_orders, 
                    total_pnl, win_rate, avg_profit_percentage, 
                    avg_loss_percentage, max_capital_required
                FROM strategy_monthly_performance
            """

            params = []
            if strategy:
                query += " WHERE strategy_name = %s"
                params.append(strategy)

            query += " ORDER BY year DESC, month DESC"

            cur.execute(query, params)
            rows = cur.fetchall()

            performance = []
            for row in rows:
                performance.append({
                    "month": row[0],
                    "year": row[1],
                    "strategy_name": row[2],
                    "total_orders": row[3],
                    "profit_orders": row[4],
                    "loss_orders": row[5],
                    "expired_orders": row[6],
                    "total_pnl": format_decimal(row[7]),
                    "win_rate": format_decimal(row[8]),
                    "avg_profit_percentage": format_decimal(row[9]),
                    "avg_loss_percentage": format_decimal(row[10]),
                    "max_capital_required": format_decimal(row[11])
                })

            return {"performance": performance}

    except Exception as e:
        logger.error(f"Error getting monthly performance: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/strategy/summary", tags=["Strategy"])
async def get_strategy_summary(strategy: str = Query(None, description="Strategy name")):
    """Get summary statistics for a strategy"""
    try:
        summaries = []

        # If no specific strategy requested, get all
        strategies_to_query = [strategy] if strategy else STRATEGIES

        for strat in strategies_to_query:
            with db_service._get_cursor() as cur:
                # Get open orders count and capital
                cur.execute("""
                    SELECT COUNT(*), SUM(entry_price * quantity)
                    FROM strategy_orders
                    WHERE status = 'OPEN' AND strategy_name = %s
                """, (strat,))

                open_stats = cur.fetchone()
                open_orders = open_stats[0] or 0
                total_capital = format_decimal(open_stats[1])

                # Get total profit
                cur.execute("""
                    SELECT SUM(pnl)
                    FROM strategy_orders
                    WHERE status = 'PROFIT' AND strategy_name = %s
                """, (strat,))

                total_profit = format_decimal(cur.fetchone()[0])

                # Get total loss
                cur.execute("""
                    SELECT SUM(pnl)
                    FROM strategy_orders
                    WHERE status = 'LOSS' AND strategy_name = %s
                """, (strat,))

                total_loss = format_decimal(cur.fetchone()[0])

                # Get total orders
                cur.execute("""
                    SELECT COUNT(*)
                    FROM strategy_orders
                    WHERE strategy_name = %s
                """, (strat,))

                total_orders = cur.fetchone()[0] or 0

                # Get win rate
                cur.execute("""
                    SELECT 
                        COUNT(*) as total_closed,
                        SUM(CASE WHEN status = 'PROFIT' THEN 1 ELSE 0 END) as profit_count
                    FROM strategy_orders
                    WHERE status != 'OPEN' AND strategy_name = %s
                """, (strat,))

                win_stats = cur.fetchone()
                total_closed = win_stats[0] or 0
                profit_count = win_stats[1] or 0

                win_rate = (profit_count / total_closed * 100) if total_closed > 0 else 0

                # Build the summary
                summary = {
                    "strategy_name": strat,
                    "openOrders": open_orders,
                    "totalOrders": total_orders,
                    "totalCapital": total_capital,
                    "totalProfit": total_profit,
                    "totalLoss": total_loss,
                    "netPnL": total_profit + total_loss,
                    "winRate": round(win_rate, 2)
                }

                summaries.append(summary)

        # Return either a single summary or list based on input
        if strategy:
            return summaries[0] if summaries else {}
        else:
            return {"summaries": summaries}

    except Exception as e:
        logger.error(f"Error getting strategy summary: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/strategy/comparison", tags=["Strategy"])
async def get_strategy_comparison():
    """Get a comparison of performance between strategies"""
    try:
        # Get summary stats for all strategies
        with db_service._get_cursor() as cur:
            # Get total profit, loss, orders by strategy
            cur.execute("""
                SELECT 
                    strategy_name,
                    SUM(CASE WHEN status = 'PROFIT' THEN pnl ELSE 0 END) as total_profit,
                    SUM(CASE WHEN status = 'LOSS' THEN pnl ELSE 0 END) as total_loss,
                    COUNT(*) as total_orders,
                    SUM(CASE WHEN status = 'PROFIT' THEN 1 ELSE 0 END) as profit_orders,
                    SUM(CASE WHEN status = 'LOSS' THEN 1 ELSE 0 END) as loss_orders,
                    AVG(CASE WHEN status = 'PROFIT' THEN pnl_percentage ELSE NULL END) as avg_profit_pct,
                    AVG(CASE WHEN status = 'LOSS' THEN pnl_percentage ELSE NULL END) as avg_loss_pct
                FROM strategy_orders
                WHERE status != 'OPEN'
                GROUP BY strategy_name
            """)

            comparison = []
            for row in cur.fetchall():
                strategy_name = row[0]
                total_profit = format_decimal(row[1] or 0)
                total_loss = format_decimal(row[2] or 0)
                total_orders = row[3] or 0
                profit_orders = row[4] or 0
                loss_orders = row[5] or 0
                avg_profit_pct = format_decimal(row[6] or 0)
                avg_loss_pct = format_decimal(row[7] or 0)

                # Calculate win rate
                win_rate = (profit_orders / total_orders * 100) if total_orders > 0 else 0

                # Calculate net P&L
                net_pnl = total_profit + total_loss

                # Get max drawdown (max capital used)
                cur.execute("""
                    SELECT MAX(capital_deployed)
                    FROM strategy_daily_capital
                    WHERE strategy_name = %s
                """, (strategy_name,))

                max_capital = format_decimal(cur.fetchone()[0] or 0)

                # Calculate ROI
                roi = (net_pnl / max_capital * 100) if max_capital > 0 else 0

                comparison.append({
                    "strategy_name": strategy_name,
                    "total_orders": total_orders,
                    "profit_orders": profit_orders,
                    "loss_orders": loss_orders,
                    "win_rate": round(win_rate, 2),
                    "total_profit": total_profit,
                    "total_loss": total_loss,
                    "net_pnl": net_pnl,
                    "avg_profit_percentage": round(avg_profit_pct, 2),
                    "avg_loss_percentage": round(avg_loss_pct, 2),
                    "max_capital": max_capital,
                    "roi": round(roi, 2)
                })

            return {"comparison": comparison}

    except Exception as e:
        logger.error(f"Error getting strategy comparison: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# Run the FastAPI app with uvicorn
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8050)
