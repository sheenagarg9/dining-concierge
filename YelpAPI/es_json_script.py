import json
import yelp_scraper

# Your original JSON data as a list of dictionaries
original_data = yelp_scraper.unique_restaurants

# Counter for _id
id_counter = 1

# Prepare the output JSON data with the required format
output_data = []
for entry in original_data:
    indexed_entry = {
        "index": {"_index": "restaurant", "_id": str(id_counter)}
    }
    # Include only specific fields after the "index" line
    modified_entry = {
        "BusinessId": entry.get("id"),
        "Cuisine": entry.get("Cuisine")
    }
    output_data.append(json.dumps(indexed_entry))
    output_data.append(json.dumps(modified_entry))
    id_counter += 1

# Join the output data with newline characters
output_json_str = "\n".join(output_data)

with open("data.json", "w") as outfile:
    outfile.write(output_json_str)

print("Data has been written to data.json")
