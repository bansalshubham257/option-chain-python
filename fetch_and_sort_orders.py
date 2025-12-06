import pandas as pd
from datetime import datetime

def predict_75pct_return(ltp, delta, gamma, iv, theta, oi, volume, lot_size, hour):
    """
    Predict if a trade can reach ≥75% return
    Returns: score (0-12), prediction (High/Moderate/Low), and confidence
    """

    score = 0
    abs_delta = abs(delta)
    abs_theta = abs(theta)

    # 1. Premium condition
    if ltp <= 60:
        score += 2

    # 2. Delta sweet spot
    if 0.35 <= abs_delta <= 0.65:
        score += 2

    # 3. Gamma acceleration
    if gamma >= 0.004:
        score += 2

    # 4. IV range
    if 0.20 <= iv <= 0.34:
        score += 1

    # 5. Theta decay control
    if abs_theta <= 1.5:
        score += 1

    # 6. Open Interest liquidity
    if oi >= 150000:
        score += 1

    # 7. Volume presence
    if volume > 0:
        score += 1

    # 8. Lot size
    if lot_size <= 1000:
        score += 1

    # 9. Time of day (9 AM - 12 PM IST)
    if 9 <= hour <= 12:
        score += 1

    # Prediction and confidence
    if score >= 9:
        prediction = "HIGH - Very likely to reach ≥75% return"
        confidence = "90%+"
    elif score >= 7:
        prediction = "MODERATE - Good chance to reach ≥75% return"
        confidence = "60-80%"
    else:
        prediction = "LOW - Unlikely to reach ≥75% return"
        confidence = "<40%"

    return score, prediction, confidence

def get_user_input():
    """Get input from user"""
    print("=== Options Trade 75% Return Predictor ===\n")

    try:
        ltp = float(input("Option Premium (LTP): ").strip())
        delta = float(input("Delta (for PE use negative): ").strip())
        gamma = float(input("Gamma: ").strip())
        iv = float(input("Implied Volatility (IV): ").strip())
        theta = float(input("Theta (use negative): ").strip())
        oi = float(input("Open Interest (OI): ").strip())
        volume = float(input("Volume (0 if unknown): ").strip())
        lot_size = int(input("Lot Size: ").strip())
        hour = int(input("Trade Hour (9-17 in 24hr format): ").strip())

        return ltp, delta, gamma, iv, theta, oi, volume, lot_size, hour

    except ValueError:
        print("Error: Please enter valid numbers")
        return None

def main():
    """Main function to run the predictor"""
    inputs = get_user_input()
    if inputs is None:
        return

    ltp, delta, gamma, iv, theta, oi, volume, lot_size, hour = inputs

    print("\n" + "="*50)
    print("PREDICTION RESULTS:")
    print("="*50)

    score, prediction, confidence = predict_75pct_return(ltp, delta, gamma, iv, theta, oi, volume, lot_size, hour)

    print(f"Score: {score}/12")
    print(f"Prediction: {prediction}")
    print(f"Confidence: {confidence}")

    # Show breakdown
    print("\nSCORE BREAKDOWN:")
    print("-" * 30)
    abs_delta = abs(delta)
    abs_theta = abs(theta)

    conditions = [
        ("Premium ≤ 60", ltp <= 60, 2),
        ("Delta 0.35-0.65", 0.35 <= abs_delta <= 0.65, 2),
        ("Gamma ≥ 0.004", gamma >= 0.004, 2),
        ("IV 0.20-0.34", 0.20 <= iv <= 0.34, 1),
        ("Theta ≤ 1.5", abs_theta <= 1.5, 1),
        ("OI ≥ 150k", oi >= 150000, 1),
        ("Volume > 0", volume > 0, 1),
        ("Lot Size ≤ 1000", lot_size <= 1000, 1),
        ("Hour 9-12", 9 <= hour <= 12, 1)
    ]

    for desc, met, points in conditions:
        status = "✓" if met else "✗"
        print(f"{status} {desc}: {points if met else 0} point(s)")

# Batch prediction for multiple trades
def batch_predict(csv_file_path):
    """Predict for multiple trades from CSV file"""
    try:
        df = pd.read_csv(csv_file_path)
        results = []

        for idx, row in df.iterrows():
            score, prediction, confidence = predict_75pct_return(
                ltp=row['ltp'],
                delta=row['delta'],
                gamma=row['gamma'],
                iv=row['iv'],
                theta=row['theta'],
                oi=row['oi'],
                volume=row.get('volume', 0),
                lot_size=row['lot_size'],
                hour=pd.to_datetime(row['timestamp']).hour
            )

            results.append({
                'id': row.get('id', idx),
                'symbol': row.get('symbol', ''),
                'score': score,
                'prediction': prediction,
                'confidence': confidence
            })

        results_df = pd.DataFrame(results)
        print("\nBATCH PREDICTION RESULTS:")
        print(results_df.to_string(index=False))

        return results_df

    except Exception as e:
        print(f"Error reading CSV: {e}")
        return None

if __name__ == "__main__":
    print("Choose input method:")
    print("1. Single trade input")
    print("2. Batch from CSV file")

    choice = input("Enter choice (1 or 2): ").strip()

    if choice == "1":
        main()
    elif choice == "2":
        file_path = input("Enter CSV file path: ").strip()
        batch_predict(file_path)
    else:
        print("Invalid choice")
