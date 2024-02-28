import json
import openai
import logging
import datetime
import pandas as pd

FORMAT = '[%(asctime)s]%(levelname) %(message)s'
logging.basicConfig(format=FORMAT)


def get_risk(doc):
    '''
    Get risk assessment based on given documents.

    Args:
        documents (list): List of documents for risk assessment.

    Returns:
        str: Risk assessment response in JSON format.
    '''
    client = openai.OpenAI(
        api_key="sk-ioPnaxVR6WBuFouFvkURqw",
        base_url="https://llms.azurewebsites.net"
    )
    try:
        response = client.chat.completions.create(model="gpt-3.5", messages=[
            {
                "role": "user",
                'content': f"Based on the following articles, assess risks in terms of [trade tensions, political instability, economic sanctions, natural disasters, conflicts, and regulatory changes] for all countries mentioned in the articles. Only respond with a list of countries and their risk {doc}. Respond in JSON format."
            }
        ])
        return response.choices[0].message.content
    except Exception as e:
        logging.error(f"Error occured while getting response: {e}")
        return None


def extract_risk_data(risk, titles, date_string):
    '''
    Extract risk data and create DataFrame.

    Args:
        risk (dict): Dictionary containing risk assessment.
        titles (list): List of titles of news articles.
        date_string (str): Date string in the format 'YYYY-MM-DD'.

    Returns:
        pd.DataFrame: DataFrame containing risk data.
    '''
    # Initialize lists to store data
    dates = []
    countries = []
    risks = []
    titles_list = []
    # Iterate over the dictionary
    for country, risk_list in risk.items():
        dates.extend([date_string] * len(risk_list))
        countries.extend([country] * len(risk_list))
        risks.extend(risk_list)
        titles_list.extend([titles] * len(risk_list))
    # Create DataFrame
    return pd.DataFrame({
        'date': dates,
        'country': countries,
        'risk': risks,
        'title': titles_list
    })


def main():
    '''Process news data to assess risks and save results.'''
    # Set date filter
    date_filter = datetime.date(2022, 3, 11)
    date_string = "2022-03-11"
    # Read news data
    df = pd.read_csv("news_headers_df.csv")
    df['Date'] = pd.to_datetime(df['Date'], utc=True)
    df.sort_values(by='Date', inplace=True)
    # Remove time from date and drop duplicates
    df['Date'] = df['Date'].dt.date
    df.drop_duplicates(subset='Title', keep='first', inplace=True)
    # Filter data for specified date and limit to 10 entries
    test = df[df.Date == date_filter]
    test = test[:10]
    # Get risk assessment
    risk = get_risk(test['Text'].to_list())
    # Convert string risk into JSON
    risk = risk.replace("'", "\"")
    risk = json.loads(risk)
    # Extract risk data and create DataFrame
    out = extract_risk_data(risk, test['Title'].to_list(), date_string)
    # Save DataFrame to CSV file
    out.to_csv(f"{date_string}-output.csv", index=False, sep=";")


if __name__ == "__main__":
    main()
