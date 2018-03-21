# -*- coding: utf-8 -*-
"""
Created on Sat Mar 10 12:45:08 2018

@author: kaszo
"""
from sklearn.datasets import load_iris

from sklearn.ensemble import RandomForestClassifier

import pandas as pd

import numpy as np

np.random.seed(0)

iris = load_iris()

df = pd.DataFrame(iris.data, columns=iris.feature_names)
#df.head()

df['species'] = pd.Categorical.from_codes(iris.target, iris.target_names)
#df.head()

df['is_train'] = np.random.uniform(0, 1, len(df)) <= .70
#df.head()

train, test = df[df['is_train']==True], df[df['is_train']==False]

#print('Number of observations in the training data:', len(train))
#print('Number of observations in the test data:',len(test))

features = df.columns[:4]

y = pd.factorize(train['species'])[0]

clf = RandomForestClassifier(n_jobs=2, random_state=0)

clf.fit(train[features], y)

clf.predict(test[features])
clf.predict_proba(test[features])

#print(clf.predict(test[features]))
#print(clf.predict_proba(test[features]))

preds = iris.target_names[clf.predict(test[features])]

pd.crosstab(test['species'], preds, rownames=['Actual Species'], colnames=['Predicted Species'])

