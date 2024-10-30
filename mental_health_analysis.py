import time  # Import the time module

# Start time
start_time = time.time()

# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Step 1: Initialize Spark session
spark = SparkSession.builder \
    .appName("Mental Health Data Processing") \
    .master("local[*]") \
    .getOrCreate()

# Step 2: Load the dataset
df = spark.read.csv("mental-illnesses-prevalence.csv", header=True, inferSchema=True)

# Step 3: Preview the dataset
print("Dataset Head:")
df.show(5)

print("\nDataset Info:")
df.printSchema()

# Step 4: Filter the data (example: remove rows with missing values in a certain column)
df_filtered = df.dropna(subset=['Year'])  # Replace 'Year' with your column of interest

# Step 5: Generate Descriptive Statistics
print("\nDescriptive Statistics:")
df_filtered.describe().show()

# Step 6: Filter the data for Sudan, India, and United States
countries_of_interest = ['Sudan', 'India', 'United States']
df_filtered = df_filtered.filter(col('Entity').isin(countries_of_interest))

# Step 7: Collect data to pandas for visualization
pdf_filtered = df_filtered.toPandas()

# Step 8: Set the plot style
plt.style.use('seaborn-v0_8-darkgrid')

# Step 9: Plot each disorder over the years for Sudan, India, and the United States
disorders = [
    'Schizophrenia disorders (share of population) - Sex: Both - Age: Age-standardized',
    'Depressive disorders (share of population) - Sex: Both - Age: Age-standardized',
    'Anxiety disorders (share of population) - Sex: Both - Age: Age-standardized',
    'Bipolar disorders (share of population) - Sex: Both - Age: Age-standardized',
    'Eating disorders (share of population) - Sex: Both - Age: Age-standardized'
]

for disorder in disorders:
    plt.figure(figsize=(10, 6))
    for country in countries_of_interest:
        country_data = pdf_filtered[pdf_filtered['Entity'] == country]
        plt.plot(country_data['Year'], country_data[disorder], marker='o', label=country)
    plt.title(f'{disorder} Over the Years')
    plt.xlabel('Year')
    plt.ylabel('Share of Population')
    plt.legend()
    plt.xticks(rotation=45)
    # Save the figure instead of displaying it
    plt.savefig(f'{disorder.replace(" ", "_").replace("(", "").replace(")", "").replace("-", "_")}.png')
    plt.close()  # Close the figure to avoid display

# Step 10: Set up the box plot
plt.figure(figsize=(10, 6))
sns.boxplot(x='Entity', 
            y='Schizophrenia disorders (share of population) - Sex: Both - Age: Age-standardized', 
            data=pdf_filtered)
plt.xticks(rotation=45)
plt.title('Schizophrenia Disorders Distribution: Sudan, India, and United States')
plt.xlabel('Country')
plt.ylabel('Share of Population')
# Save the box plot as an image
plt.savefig('Schizophrenia_Disorders_Distribution.png')
plt.close()  # Close the figure to avoid display

# Step 11: Filter the data for India and create a copy
df_india = df_filtered.filter(col('Entity') == 'India').toPandas()

# Stop the Spark session
spark.stop()

# End time
end_time = time.time()

# Calculate total time taken
total_time = end_time - start_time
print(f"Total time taken to run the code: {total_time:.2f} seconds")
