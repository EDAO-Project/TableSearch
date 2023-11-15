from django.shortcuts import render
from django.http import HttpResponse
from django.template import loader
from web.models import Annotation, User, Query, Table, Task, Work, CompletedWork
import json
import os
import random
import math
import time

params_filename = 'params.json'
agreement_count = 3
tables_per_page = 5
expected_workers = 15
lock_init_file = '.init.lock'
lock_assign_file = '.assign.lock'

def acquire_init_lock():
    while os.path.exists(lock_init_file):
        continue

    open(lock_init_file, 'w')

def release_init_lock():
    os.remove(lock_init_file)

def acquire_assign_lock():
    while os.path.exists(lock_assign_file):
        continue

    open(lock_assign_file, 'w')

def release_assign_lock():
    os.remove(lock_assign_file)

# Initializes the DB data
def init_db():
    queries = Query.objects.all()

    if len(queries) == 0:
        data = get_annotation_data(params_filename)
        inserted_tables = set()

        for query in data:
            q = Query(name = query['query_id'])
            q.save()

            for table in query['tables']:
                id = table['id']

                if id not in inserted_tables:
                    t = Table(name = id)
                    t.save()
                    inserted_tables.add(id)
        
        create_tasks(data)

# Defines annotation tasks
def create_tasks(data):
    for query in data:
        table_split = list()
        tables = list()
        i = 0

        for table in query['tables']:
            if i == tables_per_page:
                table_split.append(tables)
                tables = list()
                i = 0

            tables.append(table['id'])
            i += 1

        table_split.append(tables)

        for split in table_split:
            tables_str = ''

            for table in split:
                tables_str += table + ','

            tables_str = tables_str[0:len(tables_str) - 1]
            q = Query.objects.get(name = query['query_id'])
            task = Task(query = q, tables = tables_str)
            task.save()

# Login page
def login(request):
    acquire_init_lock()
    init_db()
    release_init_lock()

    template = loader.get_template('login.html')
    return HttpResponse(template.render())

# Assigns work to a new user
# Priorities work that has already been assigned to someone, but at most agreement_count - 1 times
def assign_work(user):
    acquire_assign_lock()
    tasks = Task.objects.all()
    already_assigned = list()
    not_assigned = list()
    tasks_to_assign = math.ceil(len(tasks) / expected_workers)

    for task in tasks:
        works_for_task = Work.objects.filter(task = task)

        if len(works_for_task) == 0:
            not_assigned.append(task)

        elif len(works_for_task) < agreement_count:
            already_assigned.append(task)
    
    given_tasks = already_assigned[:tasks_to_assign]

    if len(given_tasks) < tasks_to_assign:
        given_tasks += not_assigned[:tasks_to_assign - len(given_tasks)]

    for task in given_tasks:
        work = Work(user = user, task = task)
        work.save()

    release_assign_lock()

# Returns list of maps of query, query ID, and list of tables
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

        query_id = '/home/' + query['query']
        table_dir = '/home/' + query['table-directory']
        #query_id = query['query']
        #table_dir = query['table-directory']
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

        random.shuffle(tables)
        data.append({'query': query_table, 'query_id': query_id.split('/')[-1].replace('.json', ''), 'tables': tables})

    return data

# Returns a query from the list of queries and their tables to be annotated
def get_query(query_list, query_id):
    for query in query_list:
        if query_id == query['query_id']:
            return query

    return None

# Looks for user in DB and returns it. Otherwise, it created a new entry and returns it.
def get_user_and_add(username):
    try:
        user = User.objects.get(username = username)
        return user

    except Exception as e:
        user = User(username = username)
        user.save()
        assign_work(user)
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
    
# Retrieves tables that have been annotated by a user for a query
def get_annotated_tables(query_id, user):
    try:
        rs = Annotation.objects.filter(query_id = query_id, user = user)
        table_ids = set()
        
        for result in rs:
            table_ids.add(result.table_id.replace('_score', ''))

        return table_ids

    except Exception as e:
        return set()

# Returns the total amount of tables to annotate
def progress(data, user):
    assigned = Work.objects.filter(user = user)
    count = 0

    for work in assigned:
        completed = CompletedWork.objects.filter(work = work)

        if len(completed) != 0:
            count += 1

    return str(count) + '/' + str(len(assigned))

# Returns next task for annotation or None if there is nothing left
def next_task(user, annotation_data):
    assigned = Work.objects.filter(user = user)

    for work in assigned:
        completed = CompletedWork.objects.filter(work = work)

        if len(completed) == 0:
            work_query_id = work.task.query.name
            work_tables = work.task.tables.split(',')

            for entry in annotation_data:
                if entry['query_id'] == work_query_id:
                    annotation_task = {'query_id': work_query_id, 'work_id': work.id}
                    tables = list()

                    for table in entry['tables']:
                        if table['id'] in work_tables:
                            tables.append(table)
                    
                    annotation_task['query'] = entry['query']
                    annotation_task['tables'] = tables

                    return annotation_task
    
    return None

# Annotation page
def annotate(request):
    if request.method == 'GET':
        template = loader.get_template('annotate.html')
        return HttpResponse(template.render())

    username = request.POST['uname']
    user = get_user_and_add(username)
    annotation_data = get_annotation_data(params_filename)
    annotated_tables = list(request.POST.keys())
    annotated_tables.remove('uname')

    if annotation_data is None:
        return HttpResponse("Failed reading annotationd data")

    if 'query_id' in annotated_tables:
        annotated_tables.remove('query_id')
        annotated_tables.remove('work_id')
        annotated_tables.remove('start_time')
        annotated_tables.remove('expected_annotations')

    # Save annotation scores
    if 'expected_annotations' in request.POST.keys() and len(annotated_tables) == int(request.POST['expected_annotations']):
        for annotated_table in annotated_tables:
            score = int(request.POST[annotated_table])
            query = Query.objects.get(name = request.POST['query_id'])
            table = Table.objects.get(name = annotated_table)
            annotation = Annotation(query = query, table = table, score = score, user = user)
            annotation.save()

        elapsed_time = time.time() - float(request.POST['start_time'])
        work = Work.objects.get(id = request.POST['work_id'])
        CompletedWork(work = work, elapsed_time = elapsed_time).save()
    
    annotation_task = next_task(user, annotation_data)

    if annotation_task is None:
        template = loader.get_template('all_annotated.html')
        return HttpResponse(template.render())
    
    template = loader.get_template('annotate.html')
    context = annotation_task
    context['username'] = username
    context['progress'] = progress(annotation_data, user)
    context['start_time'] = time.time()
    context['expected_annotations'] = len(annotation_task['tables'])

    return HttpResponse(template.render(context, request))