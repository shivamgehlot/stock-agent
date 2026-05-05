#feature_store/feature_repo/feature_repo/test_workflow.py
from feast import FeatureStore

if __name__ == "__main__":
    store = FeatureStore(repo_path=".")

    features = store.get_online_features(
        features=[
            "stock_features:return_1d",
            "stock_features:rsi_14",
        ],
        entity_rows=[{"ticker": "AAPL"}],
    ).to_dict()

    print(features)