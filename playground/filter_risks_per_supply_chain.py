import io
import openai
import numpy as np
import pandas as pd
from countryinfo import CountryInfo


def extract_risks_per_chain(supply_chain: pd.DataFrame, known_risks: pd.DataFrame) -> pd.DataFrame:
    """
    Extract risks per country in the supply chain based on known risks.

    Args:
    - supply_chain (pd.DataFrame): DataFrame containing supply chain information.
    - known_risks (pd.DataFrame): DataFrame containing known risks per country.

    Returns:
    - pd.DataFrame: DataFrame containing risks associated with countries in the supply chain.
    """
    supply_chain_countries = np.unique(supply_chain['location'].to_numpy())
    supply_chain_regions = [CountryInfo(
        x).info()['region'] for x in supply_chain_countries]
    supply_chain_risks = []
    for _, known_risk in known_risks.iterrows():
        known_risk_country = known_risk['country']
        if known_risk_country in supply_chain_countries or known_risk_country in supply_chain_regions:
            supply_chain_risks.append(
                {'country': known_risk_country, 'risk': known_risk['risk']})
    return pd.DataFrame(supply_chain_risks)


def get_action_items_per_risk(client, supply_chain_risks: pd.DataFrame) -> pd.DataFrame:
    """
    Get action items for each risk in the supply chain.

    Args:
    - client: OpenAI client.
    - supply_chain_risks (pd.DataFrame): DataFrame containing risks associated with countries in the supply chain.

    Returns:
    - pd.DataFrame: DataFrame containing action items for each risk.
    """
    exported_csv: str = supply_chain_risks.to_csv(index=False, sep=";")
    try:
        response = client.chat.completions.create(model="gpt-3.5", messages=[
            {
                "role": "user",
                'content': f"Based on the following list of risk factors per country in a supply chain, suggest [alternative routes, explanations] for all countries mentioned in the list. Only mention countries that are near to the countries that are mentioned. suggest alternative trade routes and countries per risk element and explain why. respond only in csv format with ; as separator. \n {exported_csv}"
            }
        ])
        response_content = response.choices[0].message.content
        return pd.read_csv(io.StringIO(response_content), sep=";")
    except:
        return None


if __name__ == "__main__":
    # Read supply chain information and known risks from CSV files
    supply_chain_info = pd.read_csv('supplychain.csv')
    known_risks = pd.read_csv('risks.csv')

    # Extract risks per country in the supply chain
    supply_chain_risks = extract_risks_per_chain(
        supply_chain_info, known_risks)

    # Save supply chain risks to a CSV file
    supply_chain_risks.to_csv(
        'supply_chain_risks_per_country.csv', index=False)

    # Initialize OpenAI client
    client = openai.OpenAI(
        api_key="sk-ioPnaxVR6WBuFouFvkURqw",
        base_url="https://llms.azurewebsites.net"
    )

    # Get action items for each risk
    action_items = get_action_items_per_risk(client, supply_chain_risks)

    # Save action items to a CSV file
    action_items.to_csv('action_items.csv', index=False, sep=";")
