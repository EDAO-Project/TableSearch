from django.shortcuts import render
from django.http import HttpResponse
from django.template import loader
from web.models import Annotation, User
import json
import os
import random

# Login page
def login(request):
    template = loader.get_template('login.html')
    return HttpResponse(template.render())

# Returns list of maps query, query ID, and list of tables
def get_annotation_data(params_file):
    data = list()
    params = None

    with open(params_file, 'r') as file:
        params = json.load(file)

    if 'queries' not in params.keys():
        return None

    for query in params['queries']:
        if 'query' not in query.keys() or 'table-directory' not in query.keys():
            return None

        query_id = query['query']
        table_dir = query['table-directory']
        query_table = list()
        tables = list()

        if (not table_dir.endswith('/')):
            table_dir += '/'
        
        # Read query into a table without prefixes
        with open(query_id, 'r') as file:
            obj = json.load(file)

            for tuple in obj['queries']:
                query_table.append(list(map(lambda e: e.split('/')[-1], tuple)))
   
        # Read each table into a list of table lists
        for table_filename in os.listdir(table_dir):
            with open(table_dir + table_filename, 'r') as file:
                obj = json.load(file)
                table = {'id': table_filename.replace('.json', ''), 'table': list()}

                for row in obj['rows']:
                    table_row = list()

                    for column in row:
                        if len(column['links']) > 0:
                            table_row.append(column['links'][0].split('/')[-1])

                        else:
                            table_row.append(column['text'])

                    table['table'].append(table_row)

                tables.append(table)

        data.append({'query': query_table, 'query_id': query_id.split('/')[-1].replace('.json', ''), 'tables': tables})

    return data

# Looks for user in DB and returns it. Otherwise, it created a new entry and returns it.
def get_user_and_add(username):
    try:
        user = User.objects.get(username = username)
        return user

    except Exception as e:
        user = User(username = username)
        user.save()
        return user

# Looks for annotations for a given query ID. If any are found, the usernames of users who made these annotations are returned.
def get_annotated_query(query_id):
    try:
        rs = Annotation.objects.filter(query_id = query_id)
        usernames = list()

        for result in rs:
            user = User.objects.get(id = result.user.id)
            usernames.append(user.username)

        return usernames

    except Exception as e:
        return list()

# Annotation page
def annotate(request):
    if request.method == 'GET':
        template = loader.get_template('annotate.html')
        return HttpResponse(template.render())
    
    username = request.POST['uname']
    user = get_user_and_add(username)
    annotated_tables = list(request.POST.keys())
    annotated_tables.remove('uname')

    if 'query_id' in annotated_tables:
        annotated_tables.remove('query_id')

    annotation_data = get_annotation_data('../params-debug.json')

    for annotated_table in annotated_tables:
        score = int(request.POST[annotated_table])
        annotation = Annotation(query_id = request.POST['query_id'], table_id = annotated_table, score = score, user = user)
        annotation.save()

    if annotation_data is None:
        return HttpResponse("Failed reading annotationd data")
    
    query_for_annotation = None

    for query in annotation_data:
        query_id = query['query_id']
        usernames_for_query = get_annotated_query(query_id)

        if len(usernames_for_query) == 0:
            query_for_annotation = query
            break

        elif username not in usernames_for_query and query_for_annotation is None:
            query_for_annotation = query

    if query_for_annotation is None:    # In this case, the user has annotated everything
        template = loader.get_template('all_annotated.html')
        return HttpResponse(template.render())

    random.shuffle(query_for_annotation['tables'])

    template = loader.get_template('annotate.html')
    context = query_for_annotation
    context['query_id'] = context['query_id']
    context['username'] = username
    return HttpResponse(template.render(context, request))