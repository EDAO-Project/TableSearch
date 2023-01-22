import argparse
import json
import pandas as pd
import pywikibot as pw
from tqdm import tqdm
from pathlib import Path
import wikipediaapi
import math

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
    '''
    Updates the `wikipage_to_categories_dict`, `category_to_num_occurrences_dict` and `unmatchable_wikipages` given the input `wikipage_name`
    '''
    try:
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
    except:
        # For some reason about try block failed. We just add input wikipage as unmatchable
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
    print("There are", len(df), "wikipages to process")

    if args.use_chunks:
        print("A total of", math.ceil(len(df) / args.chunk_size), 'chunks will be processed')
        chunk_ranges = []
        for i in range(math.ceil(len(df) / args.chunk_size)):
            start_idx = i*args.chunk_size
            end_idx = start_idx + args.chunk_size - 1
            if end_idx > len(df):
                end_idx = len(df)
            chunk_ranges.append([start_idx, end_idx])
    else:
        chunk_ranges = [[0,len(df)]]

    # Connect to the english wikipedia
    wiki = wikipediaapi.Wikipedia('en')


    # Loop over each specified wikipage and extract all its non-hidden categories
    print("\nExtracting Wikipedia data for each specified wikipage...")
    pbar = tqdm(range(len(chunk_ranges)))
    for chunk_id in pbar:
        pbar.set_description("Processing chunk: " + str(chunk_id))

        wikipage_to_categories_dict = {}
        category_to_num_occurrences_dict = {}
        wikipage_to_links_dict = {}
        link_to_num_occurrences_dict = {}
        unmatchable_wikipages = set()
        unmatchable_wikipages_dict = {'unmatchable_wikipages': []}

        chunk_df = df.iloc[chunk_ranges[chunk_id][0]:chunk_ranges[chunk_id][1]]

        for _, row in chunk_df.iterrows():
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
        chunk_output_path = args.output_dir + 'chunk_' + str(chunk_id) + '/'
        if args.mode in ['categories', 'all']:
            if args.use_chunks:
                Path(chunk_output_path).mkdir(parents=True, exist_ok=True)
                with open(chunk_output_path+'wikipage_to_categories.json', 'w', encoding='utf-8') as fp:
                    json.dump(wikipage_to_categories_dict, fp, indent=4)
                with open(chunk_output_path+'category_to_num_occurrences.json', 'w', encoding='utf-8') as fp:
                    json.dump(category_to_num_occurrences_dict, fp, indent=4)
            else:
                with open(args.output_dir+'wikipage_to_categories.json', 'w', encoding='utf-8') as fp:
                    json.dump(wikipage_to_categories_dict, fp, indent=4)
                with open(args.output_dir+'category_to_num_occurrences.json', 'w', encoding='utf-8') as fp:
                    json.dump(category_to_num_occurrences_dict, fp, indent=4)
            
            if args.mode in ['navigation_links', 'all']:
                if args.use_chunks:
                    Path(chunk_output_path).mkdir(parents=True, exist_ok=True)
                    with open(chunk_output_path, 'w', encoding='utf-8') as fp:
                        json.dump(wikipage_to_links_dict, fp, indent=4)
                    with open(chunk_output_path, 'w', encoding='utf-8') as fp:
                        json.dump(link_to_num_occurrences_dict, fp, indent=4)
                else:
                    with open(args.output_dir+'wikipage_to_links.json', 'w', encoding='utf-8') as fp:
                        json.dump(wikipage_to_links_dict, fp, indent=4)
                    with open(args.output_dir+'link_to_num_occurrences.json', 'w', encoding='utf-8') as fp:
                        json.dump(link_to_num_occurrences_dict, fp, indent=4)
        
        if args.use_chunks:
            Path(chunk_output_path).mkdir(parents=True, exist_ok=True)
            with open(chunk_output_path + 'unmatchable_wikipages.json', 'w', encoding='utf-8') as fp:
                json.dump(unmatchable_wikipages_dict, fp, indent=4)
        else:
            with open(args.output_dir+'unmatchable_wikipages.json', 'w', encoding='utf-8') as fp:
                json.dump(unmatchable_wikipages_dict, fp, indent=4)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--wikipages_df', help='Path to the wikipages_df file specifying all wikipages in the dataset.', required=True)
    parser.add_argument('--output_dir', help='Path to where the output files will will be saved', required=True)

    parser.add_argument('--mode', choices=['categories', 'navigation_links', 'all'], default='categories', 
        help='Mode used to extract semantic attributes given a wikipage.'
    )

    parser.add_argument('--use_chunks', action='store_true',
        help='If specified then the input dataframe is processed in chunks of size --chunk_size'
    )

    parser.add_argument('--chunk_size', default=500, type=int, help='Number of wikipages to process in a single chunk')
    parser.add_argument('--starting_chunk_number', default=0, type=int, 
        help='A non-negative integer specifying from which chunk to start the querying processing. If you want to process all wikipages \
        then start from number 0.'
    )

    args = parser.parse_args()

    # Create the query output directory if it doesn't exist
    Path(args.output_dir).mkdir(parents=True, exist_ok=True)

    print("Wikipages Dataframe Path:", args.wikipages_df)
    print("Output directory:", args.output_dir)
    print("Mode:", args.mode)
    if args.use_chunks:
        print("Using chunks to process wikipages")
        print("Chunk size:", args.chunk_size)
        print("Starting chunk number", args.starting_chunk_number)
    print('\n')

    # Save the input arguments in the output_dir
    with open(args.output_dir + 'args.json', 'w') as fp:
        json.dump(vars(args), fp, sort_keys=True, indent=4)

    main(args)  