import os
import requests
from dotenv import load_dotenv
import json

"""Functions to extract player data from PUBG API.
Placeholder implementation – replace with actual logic.
"""
load_dotenv()
api_key = os.getenv("PUBG_API_KEY")

def extract_players(player_names):
    url = f"https://api.pubg.com/shards/steam/players?filter[playerNames]={player_names}"
    payload = {}
    headers = {
        "Authorization": f"Bearer {api_key}",
        "accept": "application/vnd.api+json"
    }

    print(url)   
    try:
        response = requests.get(url, headers=headers, data=payload)
        account_id = response.json()["data"][0]["id"]
        type = response.json()["data"][0]["type"]
        clanId = response.json()["data"][0]["attributes"]["clanId"]
        name = response.json()["data"][0]["attributes"]["name"]
        stats = response.json()["data"][0]["attributes"]["stats"]
        titleId = response.json()["data"][0]["attributes"]["titleId"]
        shardId = response.json()["data"][0]["attributes"]["shardId"]
        patchVersion = response.json()["data"][0]["attributes"]["patchVersion"]
        banType = response.json()["data"][0]["attributes"]["banType"]

        final_data = json.dumps({
            "account_id": account_id, 
            "type": type,
            "clanId": clanId,
            "name": name,
            "stats": stats,
            "titleId": titleId,
            "shardId": shardId,
            "patchVersion": patchVersion,
            "banType": banType
        })
        return final_data

    except Exception as e:
        print(e)
        return None
    
    
