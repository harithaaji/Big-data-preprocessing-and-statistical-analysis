# Big Data Preprocessing and Statistical Analysis of Mental Health Data Using Apache Spark

## Overview

This project focuses on the preprocessing and statistical analysis of a mental health dataset using Apache Spark. The dataset provides insights into mental health conditions, demographics, and treatment history. The aim is to explore relationships between various factors and provide statistical insights into mental health trends.

## Dataset

The dataset used for this project can be found on [Kaggle](https://www.kaggle.com/datasets/imtkaggleteam/mental-health). It contains demographic information, mental health condition details, and data related to the treatment of mental health issues.

## Objectives

- Preprocess and clean the mental health dataset for analysis.
- Perform exploratory data analysis (EDA) to uncover meaningful patterns.
- Conduct statistical analyses to evaluate relationships between demographics and mental health outcomes.
- Visualize key trends and findings in mental health.

## Tools & Technologies

- **Apache Spark** (PySpark) for big data processing and scalable analysis.
- **Python** for scripting and data analysis.
- **Pandas** for data manipulation.
- **Matplotlib** and **Seaborn** for data visualization.
- **Scikit-learn** for machine learning algorithms (if needed).
- **Jupyter Notebooks** for interactive data exploration.

## Prerequisites

Ensure you have the following installed:

- Python 3.6+
- Apache Spark 3.0+
- Jupyter Notebook (optional, for interactive analysis)
  
Install required Python libraries:


```pip install pyspark pandas matplotlib seaborn```

## Project Workflow

**1. Data Preprocessing**

The data_preprocessing.py script cleans and prepares the dataset for analysis:

```python src/data_preprocessing.py```

**2. Exploratory Data Analysis (EDA)**

Run the EDA.ipynb Jupyter Notebook to perform data exploration and generate visualizations:

```jupyter notebook notebooks/EDA.ipynb```

**3. Statistical Analysis**

To conduct statistical analysis, run the stat_analysis.py script:

```python src/stat_analysis.py```

## Results

The analysis will produce insights regarding:

Correlations between demographic factors and mental health outcomes.
The role of treatment history in influencing mental health.
Visualizations highlighting trends in mental health conditions.
