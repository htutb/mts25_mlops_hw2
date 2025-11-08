import pandas as pd
import logging
from catboost import CatBoostClassifier

# Настройка логгера
logger = logging.getLogger(__name__)

logger.info('Importing pretrained model...')

# Import model
model = CatBoostClassifier()
model.load_model('./models/my_catboost.cbm')

# Define optimal threshold
model_th = 0.98
logger.info('Pretrained model imported successfully...')


def make_pred(dt):

    print(dt.dtypes)

    expected_categorical = [
        'hour', 'year', 'month', 'day_of_month', 'day_of_week',
        'gender_cat', 'merch_cat', 'cat_id_cat',
        'one_city_cat', 'us_state_cat', 'jobs_cat'
    ]
    for col in expected_categorical:
        if col in dt.columns:
            dt[col] = dt[col].astype(str)

    dt = dt.reindex(columns=model.feature_names_, fill_value=0)

    # Make submission dataframe
    preds = model.predict_proba(dt)[:, 1]
    submission = pd.DataFrame({
        'score': preds,
        'fraud_flag': (preds > model_th).astype(int)
    })

    return submission