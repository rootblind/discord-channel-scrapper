import json
import os

if __name__ == '__main__':
    index = 0
    dataset = []
    with open("dataset.json", "w", encoding="utf-8") as writer:
        for file in os.listdir("./dump"):
            if file.endswith('.json'):
                file_path = os.path.join("./dump", file)

                with open(file_path, "r") as f:
                    data = json.load(f)
                    for conversation in data:
                        conversation["conversation_id"] = index
                        index += 1

                    dataset.extend(data)
        json.dump(dataset, writer, indent=2)

                    