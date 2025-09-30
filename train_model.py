import pandas as pd
import pickle
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer

# Load dataset
df = pd.read_csv("/Users/shubhambansal/IdeaProjects/option-chain-python/data_exports/options_orders_20250926_075139.csv")

# Features to use
features = [
    'strike_price', 'ltp', 'oi', 'volume',
    'vega', 'theta', 'gamma', 'delta', 'iv',
    'lot_size', 'pop', 'pcr', 'bid_qty', 'ask_qty'
]

# Add one-hot for option_type (CE/PE)
X = df[features].copy()
X = pd.concat([X, pd.get_dummies(df['option_type'], prefix="opt")], axis=1)

# Imputer for missing values
imp = SimpleImputer(strategy="median")
X_imp = imp.fit_transform(X)

# Save the imputer + feature list
pickle.dump(imp, open("models/imputer.pkl", "wb"))
pickle.dump(X.columns.tolist(), open("models/feature_columns.pkl", "wb"))

# Train models for each target
targets = ['is_greater_than_25pct', 'is_greater_than_50pct', 'is_greater_than_75pct']

for t in targets:
    if t not in df.columns:
        print(f"Skipping {t}, not found in CSV")
        continue

    y = df[t].astype(int)
    if y.nunique() < 2:
        print(f"Skipping {t}, only one class present")
        continue

    X_train, X_test, y_train, y_test = train_test_split(
        X_imp, y, test_size=0.2, random_state=42, stratify=y
    )

    clf = RandomForestClassifier(n_estimators=200, random_state=42)
    clf.fit(X_train, y_train)

    # Save model
    pickle.dump(clf, open(f"models/{t}_rf.pkl", "wb"))
    print(f"Model trained and saved: {t}")
