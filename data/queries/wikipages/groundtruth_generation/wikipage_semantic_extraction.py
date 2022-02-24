import argparse
import json
import pandas as pd
import pywikibot as pw
from tqdm import tqdm
from pathlib import Path
import wikipediaapi

import requests
import bs4

'''
Python script to extract the categories for each wikipage specified

python wikipage_semantic_extraction.py \
--wikipages_df ../../../tables/wikipages/wikipages_dataset/wikipages_df.pickle \
--output_dir wikipage_relevance_scores/wikipages_dataset/ --mode navigation_links
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

def get_nav_bar_links(html):
    '''
    Given the html content of a wikipedia page return as a list all the links found in its navigation bar 
    '''
    links = []
    navigation_divs = html.find_all("div", {"role": "navigation"})

    # Loop over all divs with role navigation
    for div in navigation_divs:

        # Remove the links from the navbar-mini div (they are template links with no semantic information)
        for template in div.find_all("div", {"class": "navbar-mini"}):
            template.decompose()

        # Extract all hrefs from the div
        for link in div.find_all('a', href=True):
            href = link['href']
            # Ensure this is a link to another wikipedia page (usually wikipedia pages hrefs are specified starting with './' )
            if href[0:2] == './':
                links.append(link['href'][2:])

    return links

def perform_categories_search(wiki, wikipage_name, wikipage_to_categories_dict, category_to_num_occurrences_dict, unmatchable_wikipages):
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
        unmatchable_wikipages.add(wikipage_name)

def perform_navigation_links_search(wikipage_name, wikipage_to_links_dict, link_to_num_occurrences_dict, unmatchable_wikipages):
    # Create a request to wikipedia, if it fails then add the page to the `unmatchable_wikipages` set
    
    response = requests.get("https://en.wikipedia.org/api/rest_v1/page/html/" + wikipage_name + "?redirect=true")
    if response.status_code == 200:
        html = bs4.BeautifulSoup(response.text, 'html.parser')
        navigation_links = get_nav_bar_links(html)
        wikipage_to_links_dict[wikipage_name] = navigation_links


        for link in navigation_links:
            if link not in link_to_num_occurrences_dict:
                link_to_num_occurrences_dict[link] = 1
            else:
                link_to_num_occurrences_dict[link] += 1
    else:
        # Specified wikipage could not be retrieved from wikipedia
        unmatchable_wikipages.add(wikipage_name)

def main(args):
    df = pd.read_pickle(args.wikipages_df)

    # Connect to the english wikipedia
    wiki = wikipediaapi.Wikipedia('en')

    wikipage_to_categories_dict = {}
    category_to_num_occurrences_dict = {}
    wikipage_to_links_dict = {}
    link_to_num_occurrences_dict = {}
    unmatchable_wikipages = set()
    unmatchable_wikipages_dict = {'unmatchable_wikipages': []}

    # Loop over each specified wikipage and extract all its non-hidden categories
    print("\nExtracting Wikipedia data for each specified wikipage...")
    for _, row in tqdm(df.iterrows(), total=len(df.index)):
        wikipage_name = row['wikipage'].split('/')[-1]
        
        if args.mode in ['categories', 'all']:
            perform_categories_search(
                wiki=wiki, wikipage_name=wikipage_name,
                wikipage_to_categories_dict=wikipage_to_categories_dict,
                category_to_num_occurrences_dict=category_to_num_occurrences_dict,
                unmatchable_wikipages=unmatchable_wikipages
            )
        if args.mode in ['navigation_links', 'all']:
            perform_navigation_links_search(
                wikipage_name=wikipage_name, wikipage_to_links_dict=wikipage_to_links_dict,
                link_to_num_occurrences_dict=link_to_num_occurrences_dict,
                unmatchable_wikipages=unmatchable_wikipages
            )

    unmatchable_wikipages_dict = {'unmatchable_wikipages': list(unmatchable_wikipages)}

    # Save the dictionaries into json files
    if args.mode in ['categories', 'all']:
        with open(args.output_dir+'wikipage_to_categories.json', 'w', encoding='utf-8') as fp:
            json.dump(wikipage_to_categories_dict, fp, indent=4)
        with open(args.output_dir+'category_to_num_occurrences.json', 'w', encoding='utf-8') as fp:
            json.dump(category_to_num_occurrences_dict, fp, indent=4)
    if args.mode in ['navigation_links', 'all']:
        with open(args.output_dir+'wikipage_to_links.json', 'w', encoding='utf-8') as fp:
            json.dump(wikipage_to_links_dict, fp, indent=4)
        with open(args.output_dir+'link_to_num_occurrences.json', 'w', encoding='utf-8') as fp:
            json.dump(link_to_num_occurrences_dict, fp, indent=4)

    with open(args.output_dir+'unmatchable_wikipages.json', 'w', encoding='utf-8') as fp:
        json.dump(unmatchable_wikipages_dict, fp, indent=4)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--wikipages_df', help='Path to the wikipages_df file specifying all wikipages in the dataset.', required=True)
    parser.add_argument('--output_dir', help='Path to where the output files will will be saved', required=True)

    parser.add_argument('--mode', choices=['categories', 'navigation_links', 'all'], default='categories', 
        help='Mode used to extract semantic attributes given a wikipage.')

    args = parser.parse_args()

    # Create the query output directory if it doesn't exist
    Path(args.output_dir).mkdir(parents=True, exist_ok=True)

    print("Wikipages Dataframe Path:", args.wikipages_df)
    print("Output directory:", args.output_dir)
    print("Mode:", args.mode, '\n')

    # Save the input arguments in the output_dir
    with open(args.output_dir + 'args.json', 'w') as fp:
        json.dump(vars(args), fp, sort_keys=True, indent=4)

    main(args)  