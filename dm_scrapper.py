import requests
import json
from dotenv import load_dotenv
import os
import time
import re
from datetime import datetime
import math
import random

def request_messages(token, channel_id, limit=None):
    headers= {
        'authorization': token
    }

    messages = []
    last_id = None

    # first iteration is done outside the loop since the response is different
    req = requests.get(f'https://discord.com/api/v9/channels/{channel_id}/messages', headers=headers)
    res = json.loads(req.text)


    if req.status_code != 200:
        print(f"Error: {req.status_code} - {req.text}")
        return messages

    if len(res) < 1:
        print("End of DM reached.")
        return messages
    
    for msg in res:
        messages.append(
            {
                "content": msg["content"],
                "author_id": msg["author"]["id"],
                "message_id": msg["id"],
                "timestamp": math.floor(datetime.fromisoformat(msg["timestamp"]).timestamp())
            }
        )

    last_id = messages[-1]["message_id"]
    print(f"Fetched {len(messages)} messages. Last ID: {last_id} | Content: {messages[-1]["content"]}")

    delay = round(random.uniform(1, 4), 4)
    delay_increment = 0

    while True:
        time.sleep(delay)
        delay = round(random.uniform(1, 4), 4)
        req = requests.get(f"https://discord.com/api/v9/channels/{channel_id}/messages/search?max_id={last_id}", headers=headers)
        res = json.loads(req.text)
        
        if req.status_code != 200:
            print(f"Error: {req.status_code} - {req.text}")
            if req.status_code == 429:
                delay = 20 + 5 * delay_increment
                delay_increment += 1
                print(f"Taking {delay}s break...")
                continue
            else:
                break

        if len(res["messages"]) < 1:
            print(f"End of DM reached.")
            break

        for msg in res["messages"]:
            if len(msg[0]["content"]) > 0:
                messages.append(
                    {
                        "content": msg[0]["content"],
                        "author_id": msg[0]["author"]["id"],
                        "message_id": msg[0]["id"],
                        "timestamp": math.floor(datetime.fromisoformat(msg[0]["timestamp"]).timestamp())
                    }
                )

        last_id = messages[-1]["message_id"]
        print(f"Fetched {len(messages)} messages in {delay}s. Last ID: {last_id} | Content: {messages[-1]["content"]}")

        if limit and len(messages) > limit:
            print(f"Limit exceeded.")
            break
        

    return messages

def filter_text(text, patterns=None):
        text = text.lower()
        if len(text) < 2:
            return text
        text = text.replace('\n', ' ').replace('\r', ' ')
        text = text.replace('ă', 'a')
        text = text.replace('î', 'i')
        text = text.replace('ș', 's')
        text = text.replace('ț', 't')
        text = text.replace('â', 'a')

        if patterns:
            for pattern in patterns:
                text = re.sub(pattern, '', text)
        
        text = text.lstrip()
        return text
    
def filter_data(data):
    # curate data
    patterns = [
            re.compile(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'),
            re.compile(r'<:(\d+):>'),
            re.compile(r'[^a-zA-Z -?!.,]'),
        ]
    
    filtered_data = []

    for message in data:
        message["content"] = filter_text(message["content"], patterns)

        if len(message["content"]) > 1:
            filtered_data.append(message)

    return list(reversed(filtered_data))

def format_data(data, client):
    # the neccessary processing before converting the data to dataframe

    # concatenate consecutive messages from the same author, even index will be the user label, odd index will be the response
    conversation_data = []
    last_message = data[0]

    for message in data[1:]:
        if last_message["author_id"] == message["author_id"]: # same author means the two messages should be combined
            last_message["content"] += f" {message["content"]}"
        
        if message["content"] == data[-1]["content"] or last_message["author_id"] != message["author_id"]:
            conversation_data.append(last_message)
            last_message = message # updating the new beginning of the response
    
    
    if conversation_data[0]["author_id"] == client:
        conversation_data = conversation_data[1:] # the conversation should always be started by user input

    threshold = 3600 # the threshold is used to make the difference between conversations
    # if the current message input timestamp minus last message timestamp is greater than the threshold, then a new conversation is started

    # pairing messages in input-response format
    paired_data = []
    for i in range(0, len(conversation_data), 2):
        if i < len(conversation_data) - 1:
            if conversation_data[i + 1]["timestamp"] - conversation_data[i]["timestamp"] <= threshold: # do not pair responses that are too delayed
                paired_data.append(
                    {
                        "user": conversation_data[i]["content"],
                        "response": conversation_data[i + 1]["content"],
                        "timestamp": conversation_data[i + 1]["timestamp"]
                    }
                )

    
    # grouping pairs in conversation_ids
    labeled_data = []
    last_timestamp = None
    for pair in paired_data:
        if last_timestamp == None or pair["timestamp"] - last_timestamp > threshold:
            # if this is the first pair or the difference between current and last message exceeds the threshold
            labeled_data.append(
                {
                    "conversation_id": len(labeled_data),
                    "turns": [pair]
                }
            )
        elif pair["timestamp"] - last_timestamp <= threshold:
            labeled_data[-1]["turns"].append(pair)
        
        last_timestamp = pair["timestamp"]

    return labeled_data

if __name__ == "__main__":
    load_dotenv()
    token = os.getenv("TOKEN")
    channel = os.getenv("CHANNEL")
    client = os.getenv("CLIENT_ID")

    data = request_messages(token, channel)
    data = filter_data(data)

    data = format_data(data, client)

    with open(f"./dump/{channel}.json", "w") as f:
        json.dump(data, f, indent=2)
    
    