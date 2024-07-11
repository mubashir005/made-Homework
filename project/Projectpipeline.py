import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
import os  

def load_co2_data(url):
    co2_df = pd.read_csv(url)
    co2_df = co2_df.melt(id_vars=['Country Name', 'Country Code'], var_name='Year', value_name='CO2 Emissions')
    co2_df['Year'] = co2_df['Year'].astype(int)
    return co2_df

def load_temperature_data(url):
    temp_df = pd.read_csv(url)
    temp_df['Year'] = pd.to_datetime(temp_df['dt']).dt.year
    global_temp = temp_df.groupby('Year')['AverageTemperature'].mean().reset_index()
    return global_temp

def merge_datasets(co2_df, temp_df):
    merged_df = pd.merge(co2_df, temp_df, on='Year', how='inner')
    return merged_df

def calculate_correlation(df):
    correlation = df[['CO2 Emissions', 'AverageTemperature']].corr().iloc[0, 1]
    return correlation

def add_impact_analysis(df):
    impact_data = {
        'Time Period': [
            'Early 20th Century (1900-1950)',
            'Late 20th Century (1950-2000)',
            'Early 21st Century (2000-2013)'
        ],
        'Average Temperature Increase per Decade': [0.1, 0.2, 0.3]
    }
    impact_df = pd.DataFrame(impact_data)
    df['Time Period'] = pd.cut(df['Year'], 
                               bins=[1899, 1950, 2000, 2013], 
                               labels=['Early 20th Century (1900-1950)', 
                                       'Late 20th Century (1950-2000)', 
                                       'Early 21st Century (2000-2013)'],
                               right=False)
    merged_df = pd.merge(df, impact_df, on='Time Period', how='left')
    merged_df['Time Period'] = merged_df['Time Period'].astype(str) 
    return merged_df

def load_data_to_sqlite(df, db_file, table_name):
    db_file_abs = os.path.abspath(db_file) 
    conn = sqlite3.connect(db_file_abs)
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()

def visualize_impact(df):
    fig, axes = plt.subplots(1, 2, figsize=(14, 6)) 
    
    # Bar plot for impact analysis
    axes[0].bar(df['Time Period'], df['Average Temperature Increase per Decade'], color='skyblue')
    axes[0].set_xlabel('Time Period')
    axes[0].set_ylabel('Average Temperature Increase per Decade (°C)')
    axes[0].set_title('Impact of Climate Change on Temperature Increase')
    axes[0].tick_params(axis='x', rotation=45)
    
    # Scatter plot for CO2 vs Temperature
    axes[1].scatter(df['CO2 Emissions'], df['AverageTemperature'])
    axes[1].set_xlabel('CO2 Emissions')
    axes[1].set_ylabel('Average Temperature')
    axes[1].set_title('CO2 Emissions vs. Average Temperature')
    
    plt.tight_layout()
    plt.show()
    
    
    filtered_df = df[(df['CO2 Emissions'] >= 3.00) & (df['CO2 Emissions'] <= 5.00)]
     
 
    fig, ax1 = plt.subplots()

    color = 'tab:red'
    ax1.set_xlabel('Year')
    ax1.set_ylabel('CO2 Emissions per Capita', color=color)
    ax1.plot(filtered_df['Year'], filtered_df['CO2 Emissions'], color=color)  # Using filtered_df
    ax1.tick_params(axis='y', labelcolor=color)
    # Set y-axis limits for CO2 emissions
    ax1.set_ylim(3.00, 5.00) 

    ax2 = ax1.twinx()
    color = 'tab:blue'
    ax2.set_ylabel('Average Global Temperature', color=color)
    ax2.plot(filtered_df['Year'], filtered_df['AverageTemperature'], color=color)  
    ax2.tick_params(axis='y', labelcolor=color)

    fig.tight_layout()
    plt.title('CO2 Emissions (3.00-5.00) and Average Temperature Over Time')
    plt.show()

def main():
    co2_url = 'https://query.data.world/s/x5sksfhbjl3h2xfswrbolreeaguqrg?dws=00000'
    co2_df = load_co2_data(co2_url)

    temp_url = 'https://query.data.world/s/bxhv23ezqupfoqjoi67cl55i732zbz?dws=00000'
    temp_df = load_temperature_data(temp_url)

    #  Merge datasets on 'Year'
    merged_df = merge_datasets(co2_df, temp_df)

    #  Calculate correlation between CO2 emissions and average temperature
    correlation = calculate_correlation(merged_df)
    print(f"Correlation Coefficient: {correlation:.2f}")

    #  Add impact analysis to the merged dataset
    merged_df = add_impact_analysis(merged_df)

    #  Load merged and transformed data into SQLite database
    db_file = '../data/climate_data.sqlite' 
    table_name = 'climate_data'
    load_data_to_sqlite(merged_df, db_file, table_name)

    
    visualize_impact(merged_df)

if __name__ == "__main__":
    main()