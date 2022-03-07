import json
from tqdm import tqdm

input_wikipage_to_links_path = 'wikipage_relevance_scores/wikipages_expanded_dataset/wikipage_to_links.json'
output_wikipage_to_links_path = 'wikipage_relevance_scores/wikipages_expanded_dataset/wikipage_to_links.json'
prefixes_to_remove = ('Template:', 'File:', 'Wikipedia:', 'Portal:', 'Help:')


# Read the input wikipage_to_links path
with open(input_wikipage_to_links_path, 'r') as fp:
    wikipage_to_links = json.load(fp)

links_removed = 0

# Populate the clean_wikipage_to_links
clean_wikipage_to_links = {}
for wikipage in tqdm(wikipage_to_links):
    clean_wikipage_to_links[wikipage] = []
    
    links = wikipage_to_links[wikipage]
    for link in links:
        if link.startswith(prefixes_to_remove):
            # Ignore this link (i.e., remove it from the list)
            links_removed += 1
        else:
            clean_wikipage_to_links[wikipage].append(link)

print("Removed:", links_removed, 'links')

# Save the clean_wikipage_to_links to `output_wikipage_to_links_path`
with open(output_wikipage_to_links_path, 'w', encoding='utf-8') as fp:
    json.dump(clean_wikipage_to_links, fp, indent=4)