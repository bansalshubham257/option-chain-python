#!/usr/bin/env python
import pandas as pd
import numpy as np
import pickle
from services.database import DatabaseService
import argparse
from datetime import datetime, timedelta

def load_models():
    """Load the trained ML models for prediction"""
    try:
        # Load feature preprocessing components
        imp = pickle.load(open("models/imputer.pkl", "rb"))
        feature_columns = pickle.load(open("models/feature_columns.pkl", "rb"))

        # Load the trained models
        models = {}
        for target in ['is_greater_than_25pct', 'is_greater_than_50pct', 'is_greater_than_75pct']:
            try:
                models[target] = pickle.load(open(f"models/{target}_rf.pkl", "rb"))
            except Exception as e:
                print(f"Warning: Could not load model for {target}: {str(e)}")

        return imp, feature_columns, models

    except Exception as e:
        print(f"Error loading models: {str(e)}")
        return None, None, None

def fetch_option_orders(db, limit=None, status=None, days=None, symbol=None):
    """
    Fetch option orders from the database with optional filters

    Args:
        db: Database connection
        limit: Maximum number of records to fetch
        status: Filter by status (Open, Done, etc.)
        days: Only fetch records from the last N days
        symbol: Filter by specific stock symbol

    Returns:
        DataFrame containing option orders
    """
    try:
        query_parts = ["SELECT * FROM options_orders"]
        params = []

        # Build WHERE clause based on filters
        where_clauses = []

        if status:
            where_clauses.append("status = %s")
            params.append(status)

        if days:
            where_clauses.append("timestamp > %s")
            cutoff_date = datetime.now() - timedelta(days=days)
            params.append(cutoff_date)

        if symbol:
            where_clauses.append("symbol = %s")
            params.append(symbol)

        if where_clauses:
            query_parts.append("WHERE " + " AND ".join(where_clauses))

        query_parts.append("ORDER BY timestamp DESC")

        if limit:
            query_parts.append("LIMIT %s")
            params.append(limit)

        query = " ".join(query_parts)

        # Execute the query
        with db._get_cursor() as cursor:
            cursor.execute(query, params)
            rows = cursor.fetchall()

            # Get column names
            column_names = [desc[0] for desc in cursor.description]

            # Convert to DataFrame
            df = pd.DataFrame(rows, columns=column_names)

            print(f"Fetched {len(df)} option orders from database")
            return df

    except Exception as e:
        print(f"Error fetching option orders: {str(e)}")
        return pd.DataFrame()

def prepare_features(df, feature_columns):
    """Prepare the features for the model"""
    try:
        # Create dummy variables for option_type
        df_encoded = pd.get_dummies(df['option_type'], prefix="opt")

        # Combine with numeric features
        numeric_features = ["ltp", "delta", "iv", "vega", "theta", "gamma",
                           "bid_qty", "ask_qty", "lot_size", "oi", "volume", "pop", "pcr"]

        # Create a new DataFrame with just the features needed for prediction
        features_df = df[numeric_features].copy()

        # Join with the encoded option_type
        features_df = pd.concat([features_df, df_encoded], axis=1)

        # Ensure all required columns are present
        for col in feature_columns:
            if col not in features_df.columns:
                features_df[col] = 0

        # Select only the columns needed by the model
        features_df = features_df[feature_columns]

        return features_df

    except Exception as e:
        print(f"Error preparing features: {str(e)}")
        return None

def predict_probabilities(df, features_df, imp, models):
    """Calculate probabilities for each target using the trained models"""
    try:
        # Impute missing values
        X_imp = imp.transform(features_df)

        # Make predictions for each target
        for target, model in models.items():
            target_name = target.replace("is_greater_than_", "prob_")
            probabilities = model.predict_proba(X_imp)[:, 1]
            # Store probabilities as percentage values (0-100%)
            df[target_name] = probabilities * 100

        # Calculate combined score (also as percentage)
        df["combined_score"] = (
            df["prob_25pct"] * 0.25 +
            df["prob_50pct"] * 0.5 +
            df["prob_75pct"] * 0.75
        )

        return df

    except Exception as e:
        print(f"Error predicting probabilities: {str(e)}")
        return df

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Fetch and sort option orders by probability")
    parser.add_argument("--limit", type=int, default=1000, help="Maximum number of orders to fetch")
    parser.add_argument("--status", type=str, choices=["Open", "Done"], help="Filter by order status")
    parser.add_argument("--days", type=int, default=30, help="Only include orders from the last N days")
    parser.add_argument("--symbol", type=str, help="Filter by specific stock symbol")
    parser.add_argument("--target", type=str, default="combined_score",
                        choices=["prob_25pct", "prob_50pct", "prob_75pct", "combined_score"],
                        help="Target probability to sort by")
    parser.add_argument("--output", type=str, default="option_orders_probabilities.html",
                        help="Output HTML file path (default: option_orders_probabilities.html)")
    parser.add_argument("--csv", type=str, help="Optional CSV output file path")
    parser.add_argument("--top", type=int, default=1000, help="Show top N results")
    parser.add_argument("--min-prob", type=float, default=0,
                        help="Minimum probability threshold (0-100%)")

    args = parser.parse_args()

    print("Fetching and sorting option orders by probability...")

    # Initialize database connection
    db = DatabaseService()

    # Load models
    imp, feature_columns, models = load_models()
    if not all([imp, feature_columns, models]):
        print("Failed to load models. Exiting.")
        return

    # Fetch option orders
    df = fetch_option_orders(
        db,
        limit=args.limit,
        status=args.status,
        days=args.days,
        symbol=args.symbol
    )

    if df.empty:
        print("No option orders found.")
        return

    # Filter out rows with missing values in key columns
    key_features = ["ltp", "delta", "iv"]
    valid_mask = ~df[key_features].isna().any(axis=1)
    df = df[valid_mask]
    print(f"After removing rows with missing key features: {len(df)} orders")

    if df.empty:
        print("No valid orders remain after filtering.")
        return

    # Prepare features for the model
    features_df = prepare_features(df, feature_columns)

    # Calculate probabilities
    df = predict_probabilities(df, features_df, imp, models)

    # Sort by the specified target probability
    df = df.sort_values(by=args.target, ascending=False)

    # Apply minimum probability filter if specified
    if args.min_prob > 0:
        df = df[df[args.target] >= args.min_prob]
        print(f"Applied minimum {args.target} threshold of {args.min_prob:.1f}%: {len(df)} orders remain")

    # Save to CSV if path is provided
    if args.csv:
        df.to_csv(args.csv, index=False)
        print(f"Results saved to CSV: {args.csv}")

    # Select columns for display
    display_cols = [
        "symbol", "strike_price", "option_type", "ltp", "status",
        "delta", "iv", "prob_25pct", "prob_50pct", "prob_75pct", "combined_score"
    ]

    # Format data for display
    display_df = df[display_cols].copy()

    # Limit to top N results if specified
    if args.top and args.top < len(display_df):
        display_df = display_df.head(args.top)

    # Round numeric columns and format for display
    display_df['strike_price'] = display_df['strike_price'].round(1)
    display_df['ltp'] = display_df['ltp'].round(2)
    display_df['delta'] = display_df['delta'].round(3)
    display_df['iv'] = display_df['iv'].round(3)

    for col in ["prob_25pct", "prob_50pct", "prob_75pct", "combined_score"]:
        if col in display_df.columns:
            display_df[col] = display_df[col].round(1)

    # Create HTML output with styling
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Option Orders Probability Analysis</title>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <style>
            body {{
                font-family: Arial, sans-serif;
                margin: 20px;
                background-color: #f5f5f5;
            }}
            h1, h2 {{
                color: #333;
            }}
            .header {{
                background-color: #4CAF50;
                color: white;
                padding: 10px;
                border-radius: 5px 5px 0 0;
            }}
            .info-bar {{
                background-color: #f1f1f1;
                padding: 10px;
                margin-bottom: 15px;
                border-radius: 5px;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            }}
            table {{
                border-collapse: collapse;
                width: 100%;
                margin-top: 20px;
                box-shadow: 0 4px 8px rgba(0,0,0,0.1);
                border-radius: 5px;
                overflow: hidden;
            }}
            th, td {{
                text-align: left;
                padding: 12px;
            }}
            th {{
                background-color: #4CAF50;
                color: white;
                position: sticky;
                top: 0;
            }}
            tr:nth-child(even) {{
                background-color: #f2f2f2;
            }}
            tr:hover {{
                background-color: #e0f7fa;
            }}
            .high-prob {{
                background-color: #c8e6c9 !important;
            }}
            .medium-prob {{
                background-color: #fff9c4 !important;
            }}
            .search-container {{
                margin: 20px 0;
            }}
            #searchInput {{
                width: 300px;
                padding: 10px;
                border-radius: 5px;
                border: 1px solid #ddd;
                font-size: 16px;
            }}
            .filters {{
                display: flex;
                flex-wrap: wrap;
                gap: 10px;
                margin: 15px 0;
            }}
            .filter-item {{
                background-color: white;
                border: 1px solid #ddd;
                padding: 8px 12px;
                border-radius: 20px;
                cursor: pointer;
            }}
            .filter-item.active {{
                background-color: #4CAF50;
                color: white;
            }}
            @media print {{
                body {{
                    margin: 0;
                    padding: 10px;
                }}
                table {{
                    box-shadow: none;
                }}
            }}
            .prob-cell {{
                font-weight: bold;
            }}
            .high-value {{
                color: green;
            }}
            .medium-value {{
                color: orange;
            }}
            .low-value {{
                color: red;
            }}
        </style>
    </head>
    <body>
        <div class="header">
            <h1>Option Orders Probability Analysis</h1>
        </div>
        
        <div class="info-bar">
            <p><strong>Date:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            <p><strong>Total Orders:</strong> {len(display_df)}</p>
            <p><strong>Sorted by:</strong> {args.target}</p>
            {f'<p><strong>Status filter:</strong> {args.status}</p>' if args.status else ''}
            {f'<p><strong>Symbol filter:</strong> {args.symbol}</p>' if args.symbol else ''}
        </div>
        
        <div class="search-container">
            <input type="text" id="searchInput" placeholder="Search for symbols, strike prices...">
        </div>
        
        <div class="filters">
            <div class="filter-item active" onclick="filterTable('all')">All</div>
            <div class="filter-item" onclick="filterTable('CE')">Call Options</div>
            <div class="filter-item" onclick="filterTable('PE')">Put Options</div>
            <div class="filter-item" onclick="filterTable('Open')">Open Orders</div>
        </div>

        <table id="ordersTable">
            <thead>
                <tr>
                    <th onclick="sortTable(0)">Symbol</th>
                    <th onclick="sortTable(1)">Strike</th>
                    <th onclick="sortTable(2)">Type</th>
                    <th onclick="sortTable(3)">LTP</th>
                    <th onclick="sortTable(4)">Status</th>
                    <th onclick="sortTable(5)">Delta</th>
                    <th onclick="sortTable(6)">IV</th>
                    <th onclick="sortTable(7)">25% Prob</th>
                    <th onclick="sortTable(8)">50% Prob</th>
                    <th onclick="sortTable(9)">75% Prob</th>
                    <th onclick="sortTable(10)">Combined</th>
                </tr>
            </thead>
            <tbody>
    """

    # Add table rows
    for _, row in display_df.iterrows():
        # Determine cell styling based on probability values
        prob_50 = row['prob_50pct']
        row_class = ""

        if prob_50 >= 70:
            row_class = "high-prob"
        elif prob_50 >= 50:
            row_class = "medium-prob"

        # Format probability cells with color coding
        def format_prob_cell(value):
            if value >= 70:
                return f'<td class="prob-cell high-value">{value:.1f}%</td>'
            elif value >= 50:
                return f'<td class="prob-cell medium-value">{value:.1f}%</td>'
            else:
                return f'<td class="prob-cell low-value">{value:.1f}%</td>'

        html_content += f"""
                <tr class="{row_class}">
                    <td>{row['symbol']}</td>
                    <td>{row['strike_price']}</td>
                    <td>{row['option_type']}</td>
                    <td>{row['ltp']}</td>
                    <td>{row['status']}</td>
                    <td>{row['delta']:.3f}</td>
                    <td>{row['iv']:.3f}</td>
                    {format_prob_cell(row['prob_25pct'])}
                    {format_prob_cell(row['prob_50pct'])}
                    {format_prob_cell(row['prob_75pct'])}
                    {format_prob_cell(row['combined_score'])}
                </tr>
        """

    # Complete HTML
    html_content += """
            </tbody>
        </table>

        <script>
            function sortTable(n) {
                var table, rows, switching, i, x, y, shouldSwitch, dir, switchcount = 0;
                table = document.getElementById("ordersTable");
                switching = true;
                dir = "asc";
                
                while (switching) {
                    switching = false;
                    rows = table.rows;
                    
                    for (i = 1; i < (rows.length - 1); i++) {
                        shouldSwitch = false;
                        x = rows[i].getElementsByTagName("TD")[n];
                        y = rows[i + 1].getElementsByTagName("TD")[n];
                        
                        // Check if the two rows should switch place
                        if (dir == "asc") {
                            if (isNaN(x.innerHTML) ? 
                                x.innerHTML.toLowerCase() > y.innerHTML.toLowerCase() :
                                parseFloat(x.innerHTML) > parseFloat(y.innerHTML)) {
                                shouldSwitch = true;
                                break;
                            }
                        } else if (dir == "desc") {
                            if (isNaN(x.innerHTML) ? 
                                x.innerHTML.toLowerCase() < y.innerHTML.toLowerCase() :
                                parseFloat(x.innerHTML) < parseFloat(y.innerHTML)) {
                                shouldSwitch = true;
                                break;
                            }
                        }
                    }
                    
                    if (shouldSwitch) {
                        rows[i].parentNode.insertBefore(rows[i + 1], rows[i]);
                        switching = true;
                        switchcount++;
                    } else {
                        if (switchcount == 0 && dir == "asc") {
                            dir = "desc";
                            switching = true;
                        }
                    }
                }
            }

            function filterTable(filterValue) {
                var table = document.getElementById("ordersTable");
                var rows = table.getElementsByTagName("tr");
                
                // Update active filter button
                var filterButtons = document.getElementsByClassName("filter-item");
                for (var i = 0; i < filterButtons.length; i++) {
                    filterButtons[i].classList.remove("active");
                }
                event.currentTarget.classList.add("active");
                
                for (var i = 1; i < rows.length; i++) {
                    var showRow = true;
                    
                    if (filterValue === "CE" || filterValue === "PE") {
                        // Filter by option type (column index 2)
                        if (rows[i].getElementsByTagName("td")[2].textContent !== filterValue) {
                            showRow = false;
                        }
                    } else if (filterValue === "Open") {
                        // Filter by status (column index 4)
                        if (rows[i].getElementsByTagName("td")[4].textContent !== "Open") {
                            showRow = false;
                        }
                    }
                    
                    rows[i].style.display = showRow ? "" : "none";
                }
                
                // Apply search filter on top
                searchTable();
            }
            
            function searchTable() {
                var input = document.getElementById("searchInput");
                var filter = input.value.toUpperCase();
                var table = document.getElementById("ordersTable");
                var rows = table.getElementsByTagName("tr");
                
                for (var i = 1; i < rows.length; i++) {
                    if (rows[i].style.display === "none") continue; // Skip already filtered rows
                    
                    var showRow = false;
                    var cells = rows[i].getElementsByTagName("td");
                    
                    for (var j = 0; j < cells.length; j++) {
                        var cell = cells[j];
                        if (cell) {
                            if (cell.textContent.toUpperCase().indexOf(filter) > -1) {
                                showRow = true;
                                break;
                            }
                        }
                    }
                    
                    rows[i].style.display = showRow ? "" : "none";
                }
            }
            
            document.getElementById("searchInput").addEventListener("keyup", searchTable);
        </script>
    </body>
    </html>
    """

    # Save HTML file
    with open(args.output, "w") as html_file:
        html_file.write(html_content)

    print(f"\nResults saved to HTML file: {args.output}")
    print(f"Open this file in a web browser to view the formatted table.")

    # Show table preview in terminal
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    pd.set_option('display.float_format', '{:.1f}%'.format)

    print("\nPreview of top 10 results in terminal:")
    print(display_df.head(10).to_string())

if __name__ == "__main__":
    main()
