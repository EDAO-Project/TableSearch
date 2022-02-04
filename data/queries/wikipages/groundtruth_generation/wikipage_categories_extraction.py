import argparse
import json
import pandas as pd
import pywikibot as pw
from tqdm import tqdm
'''
Python script to extract the categories for each wikipage specified

python wikipage_categories_extraction.py \
--wikipages_df ../query_dataframes/minTupleWidth_all_tuplesPerQuery_all.pickle \
--output_dir ./
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


def main(args):
    df = pd.read_pickle(args.wikipages_df)

    # Connect to the english wikipedia
    site =pw.Site('en', 'wikipedia')

    wikipage_to_categories_dict = {}
    category_to_num_occurrences_dict = {}

    # Loop over each specified wikipage and extract its categories
    print("\nExtracting Wikipedia categories for each specified wikipage...")
    for _, row in tqdm(df.iterrows(), total=len(df.index)):
        wikipage_name = row['wikipage'].split('/')[-1]
        wikipage = pw.Page(site, wikipage_name)
        if wikipage.exists():
            wikipage_categories = get_categories(wikipage)
            wikipage_to_categories_dict[wikipage_name] = wikipage_categories

            # Update the category_to_num_occurences_dict
            for category in wikipage_categories:
                if category not in category_to_num_occurrences_dict:
                    category_to_num_occurrences_dict[category] = 1
                else:
                    category_to_num_occurrences_dict[category] += 1
        else:
            print("Specified Wikipage:", wikipage_name, 'could not be found!')

    # Save the dictionaries into json files
    with open(args.output_dir+'wikipage_to_categories.json', 'w', encoding='utf-8') as fp:
        json.dump(wikipage_to_categories_dict, fp, indent=4)
    with open(args.output_dir+'category_to_num_occurrences.json', 'w', encoding='utf-8') as fp:
        json.dump(category_to_num_occurrences_dict, fp, indent=4)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--wikipages_df', help='Path to the wikipages_df file summarizing the wikipages to be used.', required=True)
    parser.add_argument('--output_dir', help='Path to where the `wikipage_to_categories.json` and \
        `category_to_num_occurrences.json` files will be saved', required=True)

    args = parser.parse_args()

    main(args)  