import json
import argparse
import pandas as pd

import os
import math
from pathlib import Path
from tqdm import tqdm

'''
Python script to compute the similarity scores between wikipages based on their identified categories or navigation links
The similarity scores are used as the relevance scores for a given wikipage.
A relevance scores ranking is produced for each wikipage 

python generate_wikipage_relevance.py --wikipages_df ../../../tables/wikipages/wikipages_dataset/wikipages_df.pickle \
--input_dir wikipage_relevance_scores/wikipages_dataset/ --mode navigation_links \
--output_dir wikipage_relevance_scores/wikipages_dataset/jaccard_navigation_links/ --similarity_mode jaccard
'''

def get_score(a, b, wikipage_to_attributes, attribute_to_num_occurrences, similarity_mode='jaccard'):
    '''
    Compute the similarity between sets of categories `a` and `b`
    '''

    intersection = a & b
    union = a | b

    if similarity_mode == 'jaccard':
        if len(union) == 0:
            return 0
        return len(intersection) / len(union)
    elif similarity_mode == 'weighted':
        num_wikipages = len(wikipage_to_attributes)

        numerator_score = 0
        denominator_score = 0

        # Compute the the weighted scores by incorporating the IDF score of each category 
        for category in intersection:
            numerator_score += math.log2(num_wikipages / attribute_to_num_occurrences[category])
        for category in union:
            denominator_score += math.log2(num_wikipages / attribute_to_num_occurrences[category])

        if denominator_score == 0:
            return 0
              
        return numerator_score / denominator_score 


def compute_similarity_scores(cur_wikipage, wikipage_to_attributes, attribute_to_num_occurrences, similarity_mode, min_threshold=0.01):
    '''
    Returns a dictionary of the similarity scores of `cur_wikipage` with respect to all other wikipages
    in `wikipage_to_categories`.
    If the similarity score between `cur_wikipage` and another wikipage is less than small specified threshold then it is omitted from the returned dictionary
    '''
    similarity_scores_dict = {}
    cur_wikipage_categories = set(wikipage_to_attributes[cur_wikipage])

    for wikipage in wikipage_to_attributes:
        wikipage_categories = set(wikipage_to_attributes[wikipage])

        score = get_score(
            a=cur_wikipage_categories, b=wikipage_categories,
            wikipage_to_attributes=wikipage_to_attributes,
            attribute_to_num_occurrences=attribute_to_num_occurrences,
            similarity_mode=similarity_mode
        )

        if score > min_threshold:
            # Check if the score with respect to `wikipage` is above a minimum threshold value to add in the list of relevant wikipages
            similarity_scores_dict[wikipage] = score   

    # Sort the `similarity_scores_dict` in descending order
    similarity_scores_dict = dict(sorted(similarity_scores_dict.items(), key=lambda item: item[1], reverse=True))
    return similarity_scores_dict
    

def main(args):
    df = pd.read_pickle(args.wikipages_df)

    if args.mode == 'categories':
        with open(args.input_dir + 'category_to_num_occurrences.json') as fp:
            attribute_to_num_occurrences = json.load(fp)
        with open(args.input_dir + 'wikipage_to_categories.json') as fp:
            wikipage_to_attributes = json.load(fp)
    elif args.mode == 'navigation_links':
        with open(args.input_dir + 'link_to_num_occurrences.json') as fp:
            attribute_to_num_occurrences = json.load(fp)
        with open(args.input_dir + 'wikipage_to_links.json') as fp:
            wikipage_to_attributes = json.load(fp)

    for _, row in tqdm(df.iterrows(), total=len(df.index)):
        wikipage_name = row['wikipage'].split('/')[-1]

        # Dictionary mapping the similarity scores of the current wikipage with all other wikipages referenced in `wikipage_to_categories`
        similarity_scores_dict = {}

        # Check if wikipage_name exists in `wikipage_to_categories` if not then only itself is marked as relevant with a score of 1
        if wikipage_name in wikipage_to_attributes:
            similarity_scores_dict = compute_similarity_scores(
                cur_wikipage = wikipage_name,
                wikipage_to_attributes = wikipage_to_attributes,
                attribute_to_num_occurrences=attribute_to_num_occurrences,
                similarity_mode=args.similarity_mode
            )          

        if not similarity_scores_dict:
            # If the `similarity_scores_dict` still has not been initialized then it is only similar with itself
            similarity_scores_dict[wikipage_name] = 1

        # Save the similarity_scores_dict to the args.output_dir
        with open(args.output_dir+str(row['wikipage_id']) + '.json', 'w', encoding='utf-8') as fp:
            json.dump(similarity_scores_dict, fp, indent=4)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--wikipages_df', help='Path to the wikipages_df file specifying all wikipages in the dataset.', required=True)
   
    parser.add_argument('--input_dir', help='Path to directory where the appropriate attribute dictionaries are stored \
        (i.e., the directory containing the wikipage to categories or wikipage to navigation links dictionaries)', required=True)
    parser.add_argument('--output_dir', help='Path to where the relevance scores for each wikipage are stored', required=True)

    # TODO: Add a third mode that combines information from both he categories and navigation links
    parser.add_argument('--mode', choices=['categories', 'navigation_links'], default='categories', 
        help='Mode used to extract semantic attributes given a wikipage.')

    parser.add_argument('--similarity_mode', choices=['jaccard', 'weighted'], default='jaccard', \
        help='Mode used for computing the similarity in attributes between two wikipages. \
        If `jaccard` then the jaccard similarity is used. \
        If `weighted` then the weighted jaccard similarity based on the IDF scores of each attribute is used')
    
    args = parser.parse_args()

    # Create the query output directory if it doesn't exist (Remove all files in it if any)
    Path(args.output_dir).mkdir(parents=True, exist_ok=True)
    for f in os.listdir(args.output_dir):
        os.remove(os.path.join(args.output_dir, f))


    main(args)  