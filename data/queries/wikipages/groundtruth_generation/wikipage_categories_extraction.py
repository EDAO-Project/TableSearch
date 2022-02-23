import argparse
import json
import pandas as pd
import pywikibot as pw
from tqdm import tqdm
from pathlib import Path
import wikipediaapi

'''
Python script to extract the categories for each wikipage specified

python wikipage_categories_extraction.py \
--wikipages_df ../../../tables/wikipages/wikipages_expanded_dataset/wikipages_df.pickle \
--output_dir wikipage_relevance_scores/wikipages_expanded_dataset/
'''

def get_categories(page):
    '''
    Given a pywikibot.Page() object return as a list all its non-hidden categories
    Hidden categories are wikipedia specific categories primarily used for project maintenance
    (for more info see: https://en.wikipedia.org/wiki/Help:Category#Hiding_categories) 

    The specified `page` must exist to run this function
    '''
    categories = []
    for category in page.categories():
        if category.isHiddenCategory():
            continue
        else:
            categories.append(category.title()[9:])
    return categories

def get_categories_wikipediaapi(wiki, page):
    '''
    Returns the non-hidden categories of a `page` using the wikipediaapi
    '''
    categories = []
    for category in wiki.categories(page, clshow='!hidden'):
        categories.append(category.title()[9:])
    return categories

def main(args):
    df = pd.read_pickle(args.wikipages_df)

    # Connect to the english wikipedia
    wiki = wikipediaapi.Wikipedia('en')

    wikipage_to_categories_dict = {}
    category_to_num_occurrences_dict = {}
    unmatchable_wikipages = {'unmatchable_wikipages': []}

    # Loop over each specified wikipage and extract all its non-hidden categories
    print("\nExtracting Wikipedia categories for each specified wikipage...")
    for _, row in tqdm(df.iterrows(), total=len(df.index)):
        wikipage_name = row['wikipage'].split('/')[-1]
        wikipage = wiki.page(wikipage_name)
        if wikipage.exists():
            wikipage_categories = get_categories_wikipediaapi(wiki, wikipage)
            wikipage_to_categories_dict[wikipage_name] = wikipage_categories

            # Update the category_to_num_occurences_dict
            for category in wikipage_categories:
                if category not in category_to_num_occurrences_dict:
                    category_to_num_occurrences_dict[category] = 1
                else:
                    category_to_num_occurrences_dict[category] += 1
        else:
            # Specified wikipage could not be matched
            unmatchable_wikipages['unmatchable_wikipages'].append(wikipage_name)

    # Save the dictionaries into json files
    with open(args.output_dir+'wikipage_to_categories.json', 'w', encoding='utf-8') as fp:
        json.dump(wikipage_to_categories_dict, fp, indent=4)
    with open(args.output_dir+'category_to_num_occurrences.json', 'w', encoding='utf-8') as fp:
        json.dump(category_to_num_occurrences_dict, fp, indent=4)
    with open(args.output_dir+'unmatchable_wikipages.json', 'w', encoding='utf-8') as fp:
        json.dump(unmatchable_wikipages, fp, indent=4)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--wikipages_df', help='Path to the wikipages_df file specifying all wikipages in the dataset.', required=True)
    parser.add_argument('--output_dir', help='Path to where the `wikipage_to_categories.json` and \
        `category_to_num_occurrences.json` files will be saved', required=True)

    args = parser.parse_args()

    # Create the query output directory if it doesn't exist
    Path(args.output_dir).mkdir(parents=True, exist_ok=True)

    # Save the input arguments in the output_dir
    with open(args.output_dir + 'wikipage_categories_extraction_args.json', 'w') as fp:
        json.dump(vars(args), fp, sort_keys=True, indent=4)

    main(args)  